import { Logger } from '@nestjs/common';
import { CustomTransportStrategy, Server } from '@nestjs/microservices';
import {
  catchError,
  exhaustMap,
  from,
  interval,
  lastValueFrom,
  merge,
  Observable,
  Subscription,
  switchMap,
  timeout,
} from 'rxjs';
import { EntityManager, EntityTarget, LessThan, MoreThanOrEqual } from 'typeorm';

export class PgTransporter extends Server implements CustomTransportStrategy {
  private handlers?: Subscription;
  private activeProcesses = 0; // Add this counter
  constructor(
    private readonly em: EntityManager,
    private readonly event_map: Map<
      EntityTarget<any>,
      {
        frequent: number;
        amount: number;
        constraint?: Record<string, any>;
        timeout?: number;
      }
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
    this.handlers = this.handleListen()
      .pipe(
        switchMap(async (entity) => {

          this.activeProcesses++; // Increment before processing
          const handler = this.getHandlers().get(entity.constructor);
          console.log(this.getHandlers())
          if (!handler) {
            this.activeProcesses--; // Decrement if no handler
            return;
          }

          try {
            const r = await handler(entity);
            const { timeout: entity_timeout } =
              this.event_map.get(entity.constructor) || {};

            if (r instanceof Observable) {
              await lastValueFrom(
                r.pipe(timeout(entity_timeout || this.timeout)),
              );
            }

            entity.status = 'succeed';
          } catch (e: any) {
            if (entity.hasOwnProperty('error_msg')) {
              entity.error_msg = e instanceof Error ? e.stack : e.message;
            }
            if (entity.retry < 6) {
              // 5 times retry
              entity.status = 'pending';
            } else {
              this.error_handler_cb?.(e);

              entity.status = 'failed';
            }
          } finally {
            await this.em.save(entity.constructor, entity);
            this.activeProcesses--; // Decrement after save
          }
        }),
      )
      .subscribe();
  }

  handleListen() {
    const observables = Array.from(this.event_map.entries()).map(
      ([entityClass, { frequent: intervalTime, amount, constraint }]) =>
        interval(intervalTime).pipe(
          exhaustMap(() =>
            from(this.fetch(entityClass, amount, constraint)).pipe(
              catchError((error) => {
                Logger.error(
                  `Error fetching results for ${entityClass}:`,
                  error,
                );
                return [];
              }),
            ),
          ),
        ),
    );

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
