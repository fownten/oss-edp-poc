.PHONY: psql, mqtt, up, down, ps, trino

up:
	docker compose up -d --build

down:
	docker compose down --remove-orphans

ps:
	docker compose ps --format "table {{.Name}}\t{{.State}}\t{{.Ports}}"

psql:
	docker exec -it db psql -U postgres -d energy_db

mqtt:
	docker exec -it mqtt sh -c "apk add --no-cache mosquitto-clients && mosquitto_sub -h localhost -t 'edp/telemetry/#' -v"

trino:
	docker exec -it trino trino --catalog nessie --schema gold
