import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions } from '@nestjs/microservices';
import { PgTransporterClient } from '..';
import { AppModule } from './app.module';
import { DataSource, EntityManager } from 'typeorm';
import { ExampleJobEntity } from './example-job.entity';
import { JobSeeder } from './job-seeder';

// Create database connection
const dataSource = new DataSource({
  type: 'postgres',
  host: '0.0.0.0',
  port: 5442,
  username: 'queue_user',
  password: 'queue_password',
  database: 'queue_db',
  entities: [ExampleJobEntity],
  synchronize: true, // Only for development
});

const entityManager: EntityManager = dataSource.manager;

// Define topics/job types with processing configuration
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

  // Initialize job seeder
  const jobSeeder = new JobSeeder(entityManager);
  
  // Seed some test jobs
  await jobSeeder.seedJobs(10);

  const app = await NestFactory.createMicroservice<MicroserviceOptions>(
    AppModule,
    {
      strategy: PgTransporterClient.connect(entityManager)
        .addTopics(topics)
        .addConfig({ timeout: 60000 })
        .errorHandler((e) => {
          console.error('Job processing error:', e);
        })
        .connect(),
    },
  );

  // Graceful shutdown handler
  const gracefulShutdown = async () => {
    console.log('\n🛑 Received shutdown signal, cleaning up...');
    
    // Close the microservice
    await app.close();
    
    // Clear all jobs
    await jobSeeder.clearAllJobs();
    
    // Close database connection
    await dataSource.destroy();
    
    console.log('✅ Cleanup completed, exiting...');
    process.exit(0);
  };

  // Listen for shutdown signals
  process.on('SIGINT', gracefulShutdown);
  process.on('SIGTERM', gracefulShutdown);

  await app.listen();
  console.log('🎯 Microservice is listening and processing jobs...');
  console.log('💡 Jobs will be processed every second');
  console.log('🛑 Press Ctrl+C to stop and cleanup');
}

bootstrap().catch(console.error);