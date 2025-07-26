import { Entity, PrimaryGeneratedColumn } from 'typeorm';
import { JobQueueBaseEntity } from '..';

@Entity('example_jobs')
export class ExampleJobEntity extends JobQueueBaseEntity {
  @PrimaryGeneratedColumn()
  id!: number;
}