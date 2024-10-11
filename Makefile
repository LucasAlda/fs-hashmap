docker-image:
	docker build -f ./workers/Dockerfile -t "workers:latest" .
	docker build -f ./seeder/Dockerfile -t "seeder:latest" .
	
docker-compose-up: docker-image
	docker compose up --build