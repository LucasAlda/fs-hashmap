package middleware

import (
	"bytes"
	"encoding/gob"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Middleware struct {
	conn           *amqp.Connection
	channel        *amqp.Channel
	reviewsQueue   *amqp.Queue
	responsesQueue *amqp.Queue
	cancelled      bool
}

func NewMiddleware() (*Middleware, error) {
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	channel.Qos(
		50,    // prefetch count
		0,     // prefetch size
		false, // global
	)

	middleware := &Middleware{conn: conn, channel: channel}

	err = middleware.declare()
	if err != nil {
		return nil, err
	}

	return middleware, nil
}

func (m *Middleware) Close() error {
	m.cancelled = true
	m.channel.Close()
	return m.conn.Close()
}

func (m *Middleware) publishExchange(exchange string, key string, body interface{}) error {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)

	err := encoder.Encode(body)
	if err != nil {
		log.Fatalf("Failed to encode message: %v", err)
		return err
	}

	err = m.channel.Publish(
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        buffer.Bytes(),
		},
	)

	if err != nil && !m.cancelled {
		log.Fatalf("Failed to publish message: %v", err)
		return err
	}

	return nil
}

func (m *Middleware) publishQueue(queue *amqp.Queue, body interface{}) error {

	err := m.publishExchange("", queue.Name, body)
	if err != nil {
		log.Fatalf("Failed to publish message: %v", err)
		return err
	}

	return nil
}

func (m *Middleware) consumeQueue(q *amqp.Queue) (<-chan amqp.Delivery, error) {
	msgs, err := m.channel.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
		return nil, err
	}

	return msgs, nil
}

func (m *Middleware) bindExchange(exchange string, key string) (*amqp.Queue, error) {
	q, err := m.channel.QueueDeclare(
		key,   // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		amqp.Table{
			amqp.ConsumerTimeoutArg: 5000,
		}, // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare queue: %v", err)
		return nil, err
	}

	err = m.channel.QueueBind(
		q.Name,   // queue name
		key,      // routing key
		exchange, // exchange
		false,
		nil,
	)

	if err != nil {
		log.Fatalf("Failed to bind queue: %v", err)
		return nil, err
	}

	return &q, nil
}
