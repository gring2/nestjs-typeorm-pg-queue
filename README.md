# NestJS TypeORM PostgreSQL Queue

A PostgreSQL-based job queue transporter for NestJS microservices, providing reliable background job processing with TypeORM integration.

## Features

- 🚀 **PostgreSQL-backed job queue** - Leverages PostgreSQL for reliable job storage and processing
- 🔄 **Configurable processing** - Set custom intervals, batch sizes, and timeouts per job type
- 🛡️ **Built-in error handling** - Graceful error handling with custom error handlers
- 📦 **TypeORM integration** - Seamless integration with existing TypeORM entities
- 🎯 **NestJS microservice support** - Works as a NestJS microservice transporter
- 🧹 **Graceful shutdown** - Clean shutdown with job cleanup and database connection closing

## Installation

```bash
npm install nestjs-typeorm-pg-queue
```

## Quick Start

### 1. Create a Job Entity

```typescript
import { Entity, Column } from 'typeorm';
import { JobQueueBaseEntity } from 'nestjs-typeorm-pg-queue';

@Entity('example_jobs')
export class ExampleJobEntity extends JobQueueBaseEntity {
  @Column()
  taskName: string;

  @Column('jsonb', { nullable: true })
  payload: any;
}
```

### 2. Create a Job Handler

```typescript
import { Injectable } from '@nestjs/common';
import { MessagePattern } from '@nestjs/microservices';

@Injectable()
export class ExampleJobHandler {
  @MessagePattern('example_jobs')
  async handleJob(data: any) {
    console.log('Processing job:', data);
    // Your job processing logic here
    return { success: true };
  }
}
```

### 3. Set Up the Microservice

```typescript
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions } from '@nestjs/microservices';
import { PgTransporterClient } from 'nestjs-typeorm-pg-queue';
import { DataSource } from 'typeorm';

// Configure your database connection
const dataSource = new DataSource({
  type: 'postgres',
  host: 'localhost',
  port: 5432,
  username: 'your_username',
  password: 'your_password',
  database: 'your_database',
  entities: [ExampleJobEntity],
  synchronize: true, // Only for development
});

// Define job processing configuration
const topics = new Map([
  [ExampleJobEntity, {
    frequent: 1000,    // Check every 1 second
    amount: 5,         // Process up to 5 jobs at once
    constraint: {},    // Additional where conditions
    timeout: 30000     // Job timeout in ms
  }]
]);

async function bootstrap() {
  await dataSource.initialize();

  const app = await NestFactory.createMicroservice<MicroserviceOptions>(
    AppModule,
    {
      strategy: PgTransporterClient.connect(dataSource.manager)
        .addTopics(topics)
        .addConfig({ timeout: 60000 })
        .errorHandler((error) => {
          console.error('Job processing error:', error);
        })
        .connect(),
    },
  );

  await app.listen();
  console.log('🎯 Microservice is listening and processing jobs...');
}

bootstrap().catch(console.error);
```

## Configuration Options

### Topic Configuration

Each job type can be configured with the following options:

- `frequent`: How often to check for new jobs (in milliseconds)
- `amount`: Maximum number of jobs to process in a single batch
- `constraint`: Additional WHERE conditions for job selection
- `timeout`: Job processing timeout (in milliseconds)

### Global Configuration

- `timeout`: Global timeout for job processing

## Job Entity Base Class

Extend `JobQueueBaseEntity` for your job entities. It provides:

- `id`: Primary key
- `status`: Job status tracking
- `createdAt`: Job creation timestamp
- `updatedAt`: Last update timestamp
- Built-in status management

## Example Usage

See the `/example` directory for a complete working example including:

- Docker Compose setup with PostgreSQL
- Job entity definition
- Job handler implementation
- Job seeding utilities
- Graceful shutdown handling

### Running the Example

1. Start PostgreSQL:
```bash
cd example
docker-compose up -d
```

2. Run the example:
```bash
npm run build
./start-example.sh
```

## Development

```bash
# Build the project
npm run build

# Run tests
npm test
```

## Requirements

- Node.js >= 16
- PostgreSQL >= 12
- NestJS >= 10
- TypeORM >= 0.3

## License

ISC

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

If you encounter any issues, please file them on the [GitHub Issues](https://github.com/gring2/nestjs-typeorm-pg-queue/issues) page.
