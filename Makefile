.PHONY: help build infra-up infra-down init-kafka start dev clean logs install typecheck lint test all

# Default target
help:
	@echo "Sensors Connectivity Platform - Makefile commands:"
	@echo ""
	@echo "  make all          - Full setup: install deps, build, start infrastructure, init topics, start services"
	@echo "  make install      - Install dependencies with pnpm"
	@echo "  make build        - Build all packages and services"
	@echo "  make infra-up     - Start infrastructure (Kafka, Redis, IPFS, Robonomics)"
	@echo "  make infra-down   - Stop infrastructure"
	@echo "  make init-kafka   - Initialize Kafka topics"
	@echo "  make start        - Start all services"
	@echo "  make dev          - Run all services in development mode"
	@echo "  make typecheck    - Type-check all TypeScript files"
	@echo "  make lint         - Lint all workspaces"
	@echo "  make test         - Run all tests"
	@echo "  make clean        - Stop infrastructure and clean build artifacts"
	@echo "  make logs         - Show docker-compose logs"
	@echo ""

# Install dependencies
install:
	@echo "📦 Installing dependencies..."
	pnpm install

# Build all packages and services
build:
	@echo "🔨 Building all packages and services..."
	pnpm build

# Start infrastructure with docker-compose
infra-up:
	@echo "🚀 Starting infrastructure (Kafka, Redis, IPFS, Robonomics)..."
	docker compose up -d
	@echo "⏳ Waiting for services to be healthy..."
	@sleep 5
	@docker compose ps

# Stop infrastructure
infra-down:
	@echo "🛑 Stopping infrastructure..."
	docker compose down

# Initialize Kafka topics
init-kafka:
	@echo "📋 Initializing Kafka topics..."
	@echo "⏳ Waiting for Kafka to be ready..."
	@until docker compose exec -T kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1; do \
		echo "Waiting for Kafka..."; \
		sleep 2; \
	done
	@echo "✅ Kafka is ready, creating topics..."
	docker compose exec -T kafka bash /kafka-init-topics.sh

# Start all services (production mode)
start:
	@echo "🚀 Starting all services..."
	pnpm dev

# Run services in development mode
dev: build infra-up init-kafka
	@echo "🔧 Running services in development mode..."
	pnpm dev

# Type-check all TypeScript files
typecheck:
	@echo "🔍 Type-checking TypeScript files..."
	pnpm typecheck

# Lint all workspaces
lint:
	@echo "🔍 Linting all workspaces..."
	pnpm lint

# Run all tests
test:
	@echo "🧪 Running all tests..."
	pnpm test

# Full setup: build everything and start services
all: install build infra-up init-kafka start

# Clean build artifacts and stop infrastructure
clean:
	@echo "🧹 Cleaning up..."
	docker compose down -v
	@echo "✅ Cleanup complete"

# Show docker-compose logs
logs:
	docker compose logs -f
