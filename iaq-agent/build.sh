#!/bin/bash

# IAQ Sensor Agent Build Script
# This script provides convenient commands for building, testing, and deploying the IAQ Agent

set -e

# Configuration
IMAGE_NAME="iaq-sensor-agent"
VERSION=${VERSION:-$(date +%Y%m%d-%H%M%S)}
BUILD_DATE=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
VCS_REF=${VCS_REF:-$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Show usage
show_usage() {
    echo "IAQ Sensor Agent Build Script"
    echo ""
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  build         Build Docker image"
    echo "  test          Run tests in Docker container"
    echo "  run           Run the application with docker-compose"
    echo "  run-prod      Run production configuration"
    echo "  stop          Stop running containers"
    echo "  clean         Clean up containers and images"
    echo "  logs          Show application logs"
    echo "  shell         Open shell in running container"
    echo "  health        Check application health"
    echo "  push          Push image to registry"
    echo ""
    echo "Options:"
    echo "  --version     Specify version tag (default: timestamp)"
    echo "  --registry    Specify Docker registry"
    echo "  --help        Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 build --version 1.0.0"
    echo "  $0 run"
    echo "  $0 test"
    echo "  $0 push --registry myregistry.com"
}

# Build Docker image
build_image() {
    log_info "Building IAQ Sensor Agent Docker image..."
    log_info "Version: $VERSION"
    log_info "Build Date: $BUILD_DATE"
    log_info "VCS Ref: $VCS_REF"
    
    docker build \
        --build-arg BUILD_DATE="$BUILD_DATE" \
        --build-arg VERSION="$VERSION" \
        --build-arg VCS_REF="$VCS_REF" \
        -t "$IMAGE_NAME:$VERSION" \
        -t "$IMAGE_NAME:latest" \
        .
    
    log_success "Docker image built successfully"
    log_info "Image tags: $IMAGE_NAME:$VERSION, $IMAGE_NAME:latest"
}

# Run tests in Docker container
run_tests() {
    log_info "Running tests in Docker container..."
    
    # Build test image if needed
    if ! docker image inspect "$IMAGE_NAME:latest" >/dev/null 2>&1; then
        log_warning "Image not found, building first..."
        build_image
    fi
    
    # Run tests
    docker run --rm \
        -v "$(pwd):/app" \
        -w /app \
        "$IMAGE_NAME:latest" \
        bash -c "
            echo 'Running unit tests...'
            python test_models.py
            python test_csv_reader.py
            python test_rabbitmq_client.py
            python test_scheduler.py
            echo 'All tests completed!'
        "
    
    log_success "Tests completed successfully"
}

# Run application with docker-compose
run_application() {
    log_info "Starting IAQ Sensor Agent with docker-compose..."
    
    # Check if .env file exists
    if [ ! -f .env ]; then
        log_warning ".env file not found, copying from .env.example"
        cp .env.example .env
        log_warning "Please edit .env file with your configuration"
    fi
    
    # Start services
    docker-compose up -d
    
    log_success "Application started successfully"
    log_info "RabbitMQ Management UI: http://localhost:15672"
    log_info "Use '$0 logs' to view application logs"
    log_info "Use '$0 stop' to stop the application"
}

# Run production configuration
run_production() {
    log_info "Starting IAQ Sensor Agent in production mode..."
    
    # Check if production env file exists
    if [ ! -f .env.prod ]; then
        log_error ".env.prod file not found"
        log_info "Please create .env.prod with production configuration"
        exit 1
    fi
    
    # Start production services
    docker-compose -f docker-compose.prod.yml --env-file .env.prod up -d
    
    log_success "Production application started successfully"
    log_info "Prometheus: http://localhost:9090"
    log_info "Grafana: http://localhost:3000"
    log_info "RabbitMQ Management: http://localhost:15672"
}

# Stop running containers
stop_application() {
    log_info "Stopping IAQ Sensor Agent..."
    
    docker-compose down
    
    # Also stop production if running
    if docker-compose -f docker-compose.prod.yml ps -q >/dev/null 2>&1; then
        docker-compose -f docker-compose.prod.yml down
    fi
    
    log_success "Application stopped successfully"
}

# Clean up containers and images
clean_up() {
    log_info "Cleaning up Docker resources..."
    
    # Stop containers
    stop_application
    
    # Remove containers
    docker-compose rm -f
    
    # Remove images
    docker rmi "$IMAGE_NAME:latest" "$IMAGE_NAME:$VERSION" 2>/dev/null || true
    
    # Remove volumes (with confirmation)
    read -p "Remove data volumes? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        docker volume rm iaq-rabbitmq-data iaq-agent-logs 2>/dev/null || true
        log_info "Volumes removed"
    fi
    
    log_success "Cleanup completed"
}

# Show application logs
show_logs() {
    log_info "Showing IAQ Sensor Agent logs..."
    docker-compose logs -f iaq-agent
}

# Open shell in running container
open_shell() {
    log_info "Opening shell in IAQ Agent container..."
    
    if ! docker-compose ps iaq-agent | grep -q "Up"; then
        log_error "IAQ Agent container is not running"
        log_info "Start the application first with '$0 run'"
        exit 1
    fi
    
    docker-compose exec iaq-agent bash
}

# Check application health
check_health() {
    log_info "Checking IAQ Sensor Agent health..."
    
    # Check if containers are running
    if docker-compose ps iaq-agent | grep -q "Up"; then
        log_success "IAQ Agent container is running"
        
        # Check health status
        health_status=$(docker inspect --format='{{.State.Health.Status}}' iaq-sensor-agent 2>/dev/null || echo "unknown")
        log_info "Health status: $health_status"
        
        # Check RabbitMQ connection
        if docker-compose ps rabbitmq | grep -q "Up"; then
            log_success "RabbitMQ container is running"
        else
            log_error "RabbitMQ container is not running"
        fi
        
    else
        log_error "IAQ Agent container is not running"
    fi
}

# Push image to registry
push_image() {
    local registry=${REGISTRY:-""}
    
    if [ -z "$registry" ]; then
        log_error "Registry not specified"
        log_info "Use --registry option or set REGISTRY environment variable"
        exit 1
    fi
    
    log_info "Pushing image to registry: $registry"
    
    # Tag for registry
    docker tag "$IMAGE_NAME:$VERSION" "$registry/$IMAGE_NAME:$VERSION"
    docker tag "$IMAGE_NAME:latest" "$registry/$IMAGE_NAME:latest"
    
    # Push images
    docker push "$registry/$IMAGE_NAME:$VERSION"
    docker push "$registry/$IMAGE_NAME:latest"
    
    log_success "Images pushed successfully"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --version)
            VERSION="$2"
            shift 2
            ;;
        --registry)
            REGISTRY="$2"
            shift 2
            ;;
        --help)
            show_usage
            exit 0
            ;;
        *)
            COMMAND="$1"
            shift
            ;;
    esac
done

# Execute command
case ${COMMAND:-""} in
    build)
        build_image
        ;;
    test)
        run_tests
        ;;
    run)
        run_application
        ;;
    run-prod)
        run_production
        ;;
    stop)
        stop_application
        ;;
    clean)
        clean_up
        ;;
    logs)
        show_logs
        ;;
    shell)
        open_shell
        ;;
    health)
        check_health
        ;;
    push)
        push_image
        ;;
    "")
        log_error "No command specified"
        show_usage
        exit 1
        ;;
    *)
        log_error "Unknown command: $COMMAND"
        show_usage
        exit 1
        ;;
esac