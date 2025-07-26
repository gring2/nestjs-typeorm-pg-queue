import { Module } from '@nestjs/common';
import { ExampleJobHandler } from './example-job.handler';

@Module({
  controllers: [ExampleJobHandler],
})
export class AppModule {}