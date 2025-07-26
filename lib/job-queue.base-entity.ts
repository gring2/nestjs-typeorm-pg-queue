import {
  Column,
  CreateDateColumn,
  UpdateDateColumn,
} from 'typeorm';

export class JobQueueBaseEntity {
  @Column({ default: 0 })
  retry!: number;

  @Column({ type: 'varchar', default: 'pending' })
  status!: jobStatus;

  @Column({ type: 'jsonb' })
  payload!: Record<string, any>;

  @Column({ type: 'text', nullable:true })
  error_msg?: string;

  @CreateDateColumn()
  created_at!: Date;

  @UpdateDateColumn()
  updated_at!: Date;
}
