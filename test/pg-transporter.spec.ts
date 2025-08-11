import { execSync } from 'child_process';
import {
  DataSource,
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  EntityManager,
} from 'typeorm';
import { PgTransporterClient } from '../lib/pg-transporter';
import { jest } from '@jest/globals';
import { firstValueFrom, of } from 'rxjs';
import { delay } from 'rxjs/operators';

// --- Test Helper Functions ---
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// --- Test Entities (assuming these are defined as in the previous example) ---
@Entity()
class TestJob {
  @PrimaryGeneratedColumn('uuid')
  id!: string;

  @Column({ default: 'pending' })
  status!: 'pending' | 'active' | 'succeed' | 'failed';

  @Column({ default: 0 })
  retry!: number;

  @Column({ type: 'text', nullable: true })
  error_msg?: string;

  @CreateDateColumn()
  created_at!: Date;

  @UpdateDateColumn()
  updated_at!: Date;
}

@Entity()
class SerialJob extends TestJob {}

@Entity()
class ParallelJob extends TestJob {}

@Entity()
class FailJob extends TestJob {}

@Entity()
class SlowJob extends TestJob {}
@Entity()
class SerialDrainJob extends TestJob {}
// --- Test Suite ---
describe('PgTransporter', () => {
  let dataSource: DataSource;
  let em: EntityManager;

  // Start docker container and initialize DB connection before all tests
  beforeAll(async () => {
    console.log('Starting PostgreSQL container...');
    execSync(
      `docker compose -f ${__dirname}/docker-compose.yaml up -d --quiet-pull --wait`,
      {
        stdio: 'inherit',
        cwd: process.cwd(),
      },
    );
    console.log('Container started. Initializing DataSource...');

    dataSource = new DataSource({
      type: 'postgres',
      host: 'localhost',
      port: 5433,
      username: 'testuser',
      password: 'testpassword',
      database: 'testdb',
      entities: [TestJob, SerialJob, ParallelJob, FailJob, SlowJob, SerialDrainJob],
      synchronize: true, // Creates schema on initial connection
    });

    await dataSource.initialize();
    em = dataSource.manager;
    console.log('DataSource initialized.');
  }, 60000); // 60-second timeout for setup

  // Stop and remove the container AND its volume after all tests
  afterAll(async () => {
    await dataSource?.destroy();
    console.log('Stopping PostgreSQL container and removing volume...');
    execSync(`docker compose -f ${__dirname}/docker-compose.yml down --volumes`, {
      stdio: 'inherit',
      cwd: process.cwd(),
    });
    console.log('Container and volume destroyed.');
  });

  // Before each test, drop and recreate all tables to ensure a clean slate.
  beforeEach(async () => {
    await dataSource.synchronize(true);
  });

  it('should drain the entire queue for a serialized topic before pausing to poll', async () => {
    const processingTimestamps: number[] = [];
    const BATCH_SIZE = 2;
    const TOTAL_JOBS = 3; // More than one batch

    const transporter = PgTransporterClient.connect(em)
      .addTopics(
        new Map([
          [
            SerialDrainJob,
            {
              frequent: 500, // A long interval to prove it's not re-polling
              amount: BATCH_SIZE,
              serialize: true,
            },
          ],
        ]),
      )
      .connect();

    transporter.addHandler(SerialDrainJob, async (job: SerialDrainJob) => {
      await sleep(50); // Simulate work
      processingTimestamps.push(Date.now());
      return of(null);
    });



    transporter.listen(() => {});
    // make sure no jobs in listen.
    await sleep(1000);

    // Insert 3 jobs
    for (let i = 0; i < TOTAL_JOBS; i++) {
      await em.save(em.create(SerialDrainJob, {}));
    }

    // Wait long enough for all 3 jobs to be processed, but less than the polling interval
    await sleep(1000);

    // Assertions
    expect(processingTimestamps.length).toBe(TOTAL_JOBS);

    // Check that the time between the last job of the first batch (job #2)
    // and the first job of the second batch (job #3) is very short,
    // proving it fetched the next batch immediately.
    const timeBetweenBatches = processingTimestamps[2] - processingTimestamps[1];
    expect(timeBetweenBatches).toBeLessThan(1000); // Should be very quick, definitely not 5000ms

    const jobsInDb = await em.countBy(SerialDrainJob, { status: 'succeed' });
    expect(jobsInDb).toBe(TOTAL_JOBS);

    await transporter.close();
  }, 10000);

  it('should process jobs sequentially when serialize is true', async () => {
    const processedOrder: string[] = [];
    const transporter = PgTransporterClient.connect(em)
      .addTopics(
        new Map([
          [SerialJob, { frequent: 100, amount: 1, serialize: true }],
        ]),
      )
      .connect();

    transporter.addHandler(SerialJob, async (job: SerialJob) => {
      await sleep(50); // Simulate work
      processedOrder.push(job.id);
      return of(null);
    });

    const job1 = await em.save(em.create(SerialJob, {}));
    const job2 = await em.save(em.create(SerialJob, {}));

    transporter.listen(() => {});

    await sleep(500); // Wait for jobs to be processed

    expect(processedOrder).toEqual([job1.id, job2.id]);
    const finalJob1 = await em.findOneBy(SerialJob, { id: job1.id });
    expect(finalJob1?.status).toBe('succeed');

    await transporter.close();
  });

  it('should process jobs in parallel when serialize is false', async () => {
    const processingTimestamps: { start: number; end: number }[] = [];
    const transporter = PgTransporterClient.connect(em)
      .addTopics(
        new Map([[ParallelJob, { frequent: 100, amount: 5 }]]),
      )
      .connect();

    transporter.addHandler(ParallelJob, async (job: ParallelJob) => {
      const start = Date.now();
      await sleep(200); // Each job takes 200ms
      const end = Date.now();
      processingTimestamps.push({ start, end });
      return of(null);
    });

    await em.save([em.create(ParallelJob, {}), em.create(ParallelJob, {})]);

    transporter.listen(() => {});

    await sleep(400); // Wait for processing

    expect(processingTimestamps.length).toBe(2);
    const [job1, job2] = processingTimestamps.sort((a, b) => a.start - b.start);
    expect(job2.start).toBeLessThan(job1.end); // Proof of parallel execution

    await transporter.close();
  });

  it('should retry a failed job and eventually mark it as failed', async () => {
    let attempt = 0;
    const mockErrorHandler = jest.fn();

    const transporter = PgTransporterClient.connect(em)
      .addTopics(new Map([[FailJob, { frequent: 50, amount: 1 }]]))
      .errorHandler(mockErrorHandler)
      .connect();

    transporter.addHandler(FailJob, (job: FailJob) => {
      attempt++;
      throw new Error(`Failure on attempt ${attempt}`);
    });

    const job = await em.save(em.create(FailJob, {}));
    transporter.listen(() => {});

    await sleep(1000); // Allow time for all retries

    const finalJob = await em.findOneByOrFail(FailJob, { id: job.id });
    expect(finalJob.status).toBe('failed');
    expect(finalJob.retry).toBe(6); // 1 initial + 5 retries
    expect(finalJob.error_msg).toContain('Failure on attempt 6');
    expect(mockErrorHandler).toHaveBeenCalledTimes(1);
    expect((mockErrorHandler.mock.calls[0]?.[0] as any)?.message).toBe(
      'Failure on attempt 6',
    );

    await transporter.close();
  });

  it('should wait for active jobs to finish on close()', async () => {
    const transporter = PgTransporterClient.connect(em)
      .addTopics(new Map([[SlowJob, { frequent: 100, amount: 1 }]]))
      .connect();

    transporter.addHandler(SlowJob, async (job: SlowJob) => {
      return firstValueFrom(of(null).pipe(delay(500))); // Job takes 500ms
    });

    const job = await em.save(em.create(SlowJob, {}));
    transporter.listen(() => {});

    await sleep(150); // Wait for the job to be picked up

    const closePromise = transporter.close();
    await sleep(100); // 100ms after calling close()

    const midJob = await em.findOneByOrFail(SlowJob, { id: job.id });
    expect(midJob.status).toBe('active'); // Job should still be running

    await closePromise; // Wait for graceful shutdown

    const finalJob = await em.findOneByOrFail(SlowJob, { id: job.id });
    expect(finalJob.status).toBe('succeed'); // Now it should be done
  });
});