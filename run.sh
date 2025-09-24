#!/bin/bash

# Build andshow_help() {
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  build          Build the Docker image"
    echo "  run            Run with docker-compose (builds if needed)"
    echo "  stop           Stop all services"
    echo "  logs           Show logs from all services"
    echo "  clean          Clean up containers and images"
    echo "  shell          Open shell in adapter container"
    echo "  redis          Open Redis CLI"
    echo "  help           Show this help"
}r LoRa Adapter

set -e

echo "🚀 LoRa Adapter - Build and Run"
echo "================================"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "⚠️  No .env file found. Creating from example..."
    cp .env.example .env
    echo "📝 Please edit .env file with your configuration before running."
    echo "💡 Example configuration is already provided for development."
fi

# Function to show help
show_help() {
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  build     Build the Docker image"
    echo "  run       Run with docker compose (builds if needed)"
    echo "  stop      Stop all services"
    echo "  logs      Show logs from all services"
    echo "  clean     Clean up containers and images"
    echo "  test      Run basic tests"
    echo "  shell     Open shell in adapter container"
    echo "  redis     Open Redis CLI"
    echo "  help      Show this help"
}

case "$1" in
    "build")
        echo "🔨 Building LoRa Adapter Docker image..."
        docker compose build
        echo "✅ Build complete"
        ;;
    
    "run")
        echo "🚀 Starting LoRa Adapter services..."
        docker compose up -d
        echo "✅ Services started"
        echo ""
        echo "📊 Service Status:"
        docker compose ps
        echo ""
        echo "📝 To view logs: $0 logs"
        echo "🛑 To stop: $0 stop"
        ;;
    
    "stop")
        echo "🛑 Stopping LoRa Adapter services..."
        docker compose down
        echo "✅ Services stopped"
        ;;
    
    "logs")
        echo "📝 Showing service logs (Ctrl+C to exit)..."
        docker compose logs -f
        ;;
    
    "clean")
        echo "🧹 Cleaning up containers and images..."
        docker compose down -v --rmi all
        docker system prune -f
        echo "✅ Cleanup complete"
        ;;
    
    
    "shell")
        echo "🐚 Opening shell in adapter container..."
        docker compose exec lora-adapter /bin/bash
        ;;
    
    "redis")
        echo "📊 Opening Redis CLI..."
        docker compose exec redis redis-cli
        ;;
    
    "help"|""|*)
        show_help
        ;;
esac