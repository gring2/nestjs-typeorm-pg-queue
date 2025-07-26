import { EntityManager } from 'typeorm';
import { ExampleJobEntity } from './example-job.entity';

export class JobSeeder {
  constructor(private readonly entityManager: EntityManager) {}

  async seedJobs(count: number = 5): Promise<void> {
    console.log(`🌱 Seeding ${count} example jobs...`);
    
    const jobs: Partial<ExampleJobEntity>[] = [];
    
    for (let i = 1; i <= count; i++) {
      jobs.push({
        status: 'pending',
        retry: 0,
        payload: {
          message: `Hello from job ${i}`,
          timestamp: new Date().toISOString(),
          priority: Math.floor(Math.random() * 3) + 1, // 1-3
          data: {
            userId: `user_${i}`,
            action: `process_task_${i}`,
            metadata: { source: 'seeder', batch: 'initial' }
          }
        }
      });
    }

    await this.entityManager.save(ExampleJobEntity, jobs);
    console.log(`✅ Successfully seeded ${count} jobs`);
  }

  async clearAllJobs(): Promise<void> {
    console.log('🧹 Clearing all jobs from database...');
    const result = await this.entityManager.delete(ExampleJobEntity, {});
    console.log(`✅ Deleted ${result.affected || 0} jobs`);
  }
}