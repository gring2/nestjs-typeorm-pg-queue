import { Injectable, Logger, Controller } from '@nestjs/common';
import { MessagePattern } from '@nestjs/microservices';
import { ExampleJobEntity } from './example-job.entity';

@Controller()
@Injectable()
export class ExampleJobHandler {
  private readonly logger = new Logger(ExampleJobHandler.name);

  @MessagePattern(ExampleJobEntity)
  async handleExampleJob(job: ExampleJobEntity) {
    this.logger.log(`Processing job ${job.id} with payload:`, job.payload);
    
    // Simulate some work
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    this.logger.log(`Job ${job.id} completed successfully`);
    return { success: true, processedAt: new Date() };
  }
}