#!/bin/bash

echo "🚀 Starting NestJS TypeORM PostgreSQL Queue Example"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Start PostgreSQL container
echo "📦 Starting PostgreSQL 17 container..."
docker compose -f ./example/docker-compose.yml up -d postgres

# Wait for PostgreSQL to be ready
echo "⏳ Waiting for PostgreSQL to be ready..."
timeout 60 bash -c 'until docker-compose exec postgres pg_isready -U queue_user -d queue_db; do sleep 2; done'

if [ $? -eq 124 ]; then
    echo "❌ PostgreSQL failed to start within 60 seconds"
    exit 1
fi

echo "✅ PostgreSQL is ready!"

# Install dependencies if node_modules doesn't exist
if [ ! -d "node_modules" ]; then
    echo "📦 Installing dependencies..."
    npm install
fi

# Start the example using ts-node
echo "🎯 Starting the queue example with ts-node..."
echo "📝 The microservice will process jobs from the PostgreSQL queue"
echo "🛑 Press Ctrl+C to stop"

npx ts-node example/main.ts