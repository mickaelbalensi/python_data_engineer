
# Wiki Scraper

## Description

Wiki Scraper is a distributed web scraping system built with **RabbitMQ**, **MongoDB**, **Redis**, and **Docker**. The system fetches and processes URLs from Wikipedia, stores the fetched HTML content, and extracts relevant links. The architecture is designed with multiple services (fetcher, filter, parser) running in isolated Docker containers, ensuring scalability and efficiency.

---

## Table of Contents

1. [Features](#features)
2. [Technologies](#technologies)
3. [Setup Instructions](#setup-instructions)
4. [Configuration](#configuration)
5. [Usage](#usage)
6. [License](#license)

---

## Features

- **Distributed architecture** with multiple services running in Docker containers.
- **Persistent data storage** using MongoDB for HTML content and Redis for processed links.
- **Link processing** with RabbitMQ message queues for communication between services.
- **Scalable** to allow running multiple instances of services (like Fetcher).
- **Persistent messages** in RabbitMQ and MongoDB.

---

## Technologies

- **Python**: Programming language for the core logic and services.
- **RabbitMQ**: Message broker for communication between services.
- **MongoDB**: Database for storing fetched HTML pages.
- **Redis**: In-memory data store for managing processed links.
- **Docker**: Containerization platform for running services.
- **BeautifulSoup**: Library for extracting links from HTML pages.

---

## Setup Instructions

### Prerequisites

Before setting up the project, ensure you have the following installed:

- [Docker](https://www.docker.com/get-started)
- [Docker Compose](https://docs.docker.com/compose/install/)

### Clone the Repository

```bash
git clone <repository_url>
cd <repository_directory>
```

### Running the Project

1. **Build and start the containers** using Docker Compose:
   
   ```bash
   docker-compose up --build
   ```

2. **Scale the fetcher service** (optional):
   
   If you want to run multiple instances of the fetcher service, you can scale it like this:

   ```bash
   docker-compose up --scale fetcher=3
   ```

3. The services will start, and the system will begin processing URLs from the queue.

### Stopping the Project

To stop the project without removing logs or data:

```bash
docker-compose down
```

You can also stop individual containers:

```bash
docker stop <container_name>
```

---

## Configuration

The configuration for the services is defined in the `docker-compose.yml` file. Key variables that can be adjusted:

- **RabbitMQ connection**: The `RABBITMQ_HOST` and `RABBITMQ_PORT` environment variables control the connection to RabbitMQ.
- **MongoDB connection**: The `MONGO_URI` environment variable defines the connection to MongoDB.
- **Redis connection**: The `REDIS_HOST` and `REDIS_PORT` environment variables control the connection to Redis.
- **HTML page storage**: The `fetcher` service saves the fetched HTML pages to the local `./html_pages` directory.

---

## Usage

### Seeding the Queue

To seed the queue with an initial URL:

- This is done automatically when the `fetcher` service starts, but you can also manually seed the queue using the `seed_queue()` method from the `Fetcher` class.

### Accessing Logs

You can access the logs of a container using:

```bash
docker logs <container_name_or_id>
```

For example, to view the logs of the `fetcher` container:

```bash
docker logs fetcher-container
```

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
