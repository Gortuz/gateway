import { Injectable, OnModuleInit, OnModuleDestroy, Logger, HttpException, HttpStatus } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom, timeout, catchError, throwError } from 'rxjs';

@Injectable()
export class KafkaService implements OnModuleInit, OnModuleDestroy {
  private client: ClientKafka;
  private readonly logger = new Logger(KafkaService.name);

  constructor(private configService: ConfigService) {
    this.client = new ClientKafka({
      client: {
        clientId: this.configService.get<string>('KAFKA_CLIENT_ID', 'gateway-client'),
        brokers: this.configService.get<string>('KAFKA_BROKERS', 'localhost:9092').split(','),
      },
      consumer: {
        groupId: this.configService.get<string>('KAFKA_CONSUMER_GROUP_ID', 'gateway-consumer'),
      },
    });
  }

  async onModuleInit() {
    const topics = [
      'user.create',
      'user.find.all',
      'user.find.one',
      'user.update',
      'user.delete',
    ];

    this.logger.log('Subscribing to response topics...');
    topics.forEach((topic) => {
      this.client.subscribeToResponseOf(topic);
    });

    try {
      await this.client.connect();
      this.logger.log('✅ Kafka client connected successfully');
    } catch (error) {
      this.logger.error('❌ Failed to connect to Kafka', error.stack);
    }
  }

  async onModuleDestroy() {
    await this.client.close();
    this.logger.log('Kafka client connection closed');
  }

  async send<TResult = any, TInput = any>(pattern: string, payload: TInput): Promise<TResult> {
    this.logger.debug(`Sending message to pattern: ${pattern}`, payload);
    
    return await firstValueFrom(
      this.client.send<TResult>(pattern, payload).pipe(
        // timeout(10000), // Wait max 10 seconds for a response
        catchError((err) => {
          this.logger.error(`Error or Timeout for pattern: ${pattern}`, err.stack || err);
          return throwError(() => new HttpException(
            `Microservice timeout or error on pattern: ${pattern}`, 
            HttpStatus.GATEWAY_TIMEOUT
          ));
        })
      )
    );
  }
}
