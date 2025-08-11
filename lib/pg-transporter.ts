import { Logger } from '@nestjs/common';
import { CustomTransportStrategy, Server } from '@nestjs/microservices';
import {
  catchError, concatMap,
  exhaustMap,
  from,
  interval,
  lastValueFrom,
  merge, mergeMap,
  Observable,
  Subscription,
  timeout,
} from 'rxjs';
import { EntityManager, EntityTarget, LessThan, MoreThanOrEqual } from 'typeorm';

// Define a reusable, clear type for topic options
type TopicOptions = {
  frequent: number;
  amount: number;
  constraint?: Record<string, any>;
  timeout?: number;
  serialize?: boolean; // Use boolean for better type safety
};
export class PgTransporter extends Server implements CustomTransportStrategy {
  private handlers?: Subscription;
  private activeProcesses = 0; // Add this counter
  constructor(
    private readonly em: EntityManager,
    private readonly event_map: Map<
      EntityTarget<any>,
      TopicOptions
    >,
    private readonly timeout = 30000,
    private readonly error_handler_cb?: (error: Error) => void,
  ) {
    super();
  }

  async listen(callback: () => void) {
    // recover hanging jobs
    Logger.log('recovery start');

    for (const v of this.event_map.keys()) {
      await this.em.update(
        v,
        {
          status: 'active',
          updated_at: MoreThanOrEqual(
            new Date(Date.now() + this.timeout + 1000),
          ),
        },
        {
          status: 'pending',
        },
      );
    }
    Logger.log('recovery done');

    this.subscribeHandler();
    callback();
  }

  subscribeHandler() {
    this.handlers = this.handleListen().subscribe();
  }


  handleListen() {
    const observables = Array.from(this.event_map.entries()).map(
      ([entityClass, options]) => {
        const {
          frequent: intervalTime,
          amount,
          constraint,
          serialize,
        } = options;

        // 1. Polling Stream: Fetches a batch of jobs for this specific topic.
        // exhaustMap prevents new fetches while the current batch is still processing.
        const source$ = interval(intervalTime).pipe(
          exhaustMap(() =>
            from(this.fetch(entityClass, amount, constraint)).pipe(
              catchError((error) => {
                const entityName =
                  typeof entityClass === 'function'
                    ? entityClass.name
                    : String(entityClass);
                Logger.error(
                  `Error fetching jobs for ${entityName}:`,
                  error.stack,
                );
                return []; // Return an empty array to prevent the stream from dying
              }),
            ),
          ),
        );

        // 2. Processing Logic: A reusable function to handle a single job entity.
        const processEntity = async (entity: any) => {
          this.activeProcesses++;
          const handler = this.getHandlers().get(entity.constructor);

          if (!handler) {
            Logger.warn(`No handler found for job type: ${entity.constructor.name}`);
            this.activeProcesses--;
            return;
          }

          try {
            const result = await handler(entity);
            const { timeout: entity_timeout } =
            this.event_map.get(entity.constructor) || {};

            if (result instanceof Observable) {
              if (serialize) {
                // Serialized jobs run to completion, ignoring timeout
                await lastValueFrom(result);
              } else {
                await lastValueFrom(
                  result.pipe(timeout(entity_timeout || this.timeout)),
                );
              }
            }
            entity.status = 'succeed';
          } catch (e: any) {
            if (entity.hasOwnProperty('error_msg')) {
              entity.error_msg = e instanceof Error ? e.stack : e.message;
            }
            // Retry logic
            if (entity.retry < 6) {
              entity.status = 'pending';
            } else {
              this.error_handler_cb?.(e);
              entity.status = 'failed';
            }
          } finally {
            // Ensure the entity is always saved and the process counter is decremented
            await this.em.save(entity.constructor, entity);
            this.activeProcesses--;
          }
        };

        // 3. Attach the right processing strategy (the "Assembly Line" type)
        if (serialize) {
          // concatMap processes jobs one-by-one for this topic.
          return source$.pipe(concatMap(processEntity));
        } else {
          // mergeMap processes jobs in parallel, up to the 'amount' limit for this topic.
          return source$.pipe(mergeMap(processEntity, amount));
        }
      },
    );

    // 4. Run all topic streams concurrently.
    return merge(...observables);
  }

  async close() {
    //: Promise<void>
    Logger.log('close!!!');
    this.handlers?.unsubscribe();
    await this.waitForShutdown();
    Logger.log('done');
  }

  private async waitForShutdown(): Promise<string> {
    if (this.activeProcesses === 0) return 'closed';

    // Poll until active processes are done (or use events for efficiency)
    return new Promise((resolve) => {
      const check = setInterval(() => {
        if (this.activeProcesses === 0) {
          clearInterval(check);
          resolve('resolved!!!');
        }
      }, 10); // Check every 10ms; adjust as needed
    });
  }

  async *fetch(
    target: EntityTarget<any>,
    cnt: number,
    constraint?: Record<string, any>,
  ) {
    const repo = this.createRepo(target);
    const cte = repo
      .createQueryBuilder('job')
      .setLock('pessimistic_write')
      .setOnLocked('skip_locked')
      .where({
        status: 'pending',
        ...constraint,
        updated_at: LessThan(new Date(Date.now() - 100)),
      })
      .orderBy({ updated_at: 'ASC' })
      .limit(cnt)
      .select('id');

    const tt = repo
      .createQueryBuilder('job')
      .addCommonTableExpression(cte, 'nextJob')
      .update({
        status: 'active',
        retry: () => 'retry + 1',
      });
    const [sql, params] = tt.getQueryAndParameters();
    const query = `${sql}
                   FROM "nextJob"
                   WHERE "${repo.metadata.tableName}".id = "nextJob".id
                   RETURNING "${repo.metadata.tableName}".*
    `;

    const rows = await this.em.query(query, params);
    for (const job of rows[0].filter((r) => r)) {
      const entity = this.em.create(target, job);
      yield entity;
    }
  }

  private createRepo(target: EntityTarget<any>) {
    let repo = this.em.getRepository(target);
    const metadata = repo.metadata;
    const { parentEntityMetadata, inheritancePattern } = metadata;
    const is_child_entity =
      parentEntityMetadata?.target && inheritancePattern === 'STI';

    if (is_child_entity) {
      repo = this.em.getRepository(parentEntityMetadata.target);
    }

    return repo;
  }
}

export class PgTransporterClient {
  private em?: EntityManager;
  private event_map?: Map<
    EntityTarget<any>,
    {
      frequent: number;
      amount: number;
      constraint?: Record<string, any>;
    }
  >;
  private timeout: number = 30000;
  private error_handler_cb?: (error: Error) => void;

  static connect(em: EntityManager) {
    const client = new PgTransporterClient();

    client.em = em;
    return client;
  }

  addTopics(
    event_map: Map<
      EntityTarget<any>,
      {
        frequent: number;
        amount: number;
        constraint?: Record<string, any>;
        serialize?: true
      }
    >,
  ) {
    this.event_map = event_map;

    return this;
  }

  addConfig(config: { timeout?: number }) {
    const { timeout } = config;
    if (timeout) this.timeout = timeout;

    return this;
  }

  errorHandler(error_handler_cb: (error: Error) => void) {
    this.error_handler_cb = error_handler_cb;
    return this;
  }

  connect() {
    if (!this.em || !this.event_map) throw new Error();
    return new PgTransporter(
      this.em,
      this.event_map,
      this.timeout,
      this.error_handler_cb,
    );
  }
}
