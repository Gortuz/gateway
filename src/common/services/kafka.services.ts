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
        brokers: this.configService.get<string>('KAFKA_BROKERS', 'localhost:9091').split(','),
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
      this.logger.error('❌ Failed to connect to Kafka', error instanceof Error ? error.stack : String(error));
    }
  }

  async onModuleDestroy() {
    await this.client.close();
    this.logger.log('Kafka client connection closed');
  }

  async send<TResult = any, TInput = any>(
    pattern: string,
    payload: TInput,
  ): Promise<TResult> {
    this.logger.debug(`📤 Sending message to pattern: ${pattern}`, payload);

    try {
      const response = await firstValueFrom(
        this.client.send<TResult>(pattern, payload).pipe(
          timeout(5000),  // Timeout de 5 segundos
          catchError((err) => {
            const errorPayload = err?.error ?? err?.response ?? err?.message ?? err;

            // Timeout específico
            if (err.name === 'TimeoutError') {
              this.logger.error(
                `⏱️ Timeout for pattern: ${pattern} (after 5s, no response from microservice)`,
              );
              return throwError(
                () =>
                  new HttpException(
                    `Request timeout on pattern: ${pattern}. Microservice did not respond.`,
                    HttpStatus.GATEWAY_TIMEOUT,
                  ),
              );
            }

            // Si es un RpcException del microservicio
            if (errorPayload && typeof errorPayload === 'object') {
              this.logger.error(
                `❌ RPC Error for pattern: ${pattern}`,
                JSON.stringify(errorPayload),
              );
              return throwError(
                () => new HttpException(
                  errorPayload,
                  (errorPayload as any).statusCode ?? HttpStatus.BAD_GATEWAY,
                ),
              );
            }

            this.logger.error(
              `❌ Error for pattern: ${pattern}`,
              typeof errorPayload === 'string' ? errorPayload : JSON.stringify(errorPayload),
            );
            return throwError(
              () =>
                new HttpException(
                  typeof errorPayload === 'string' ? errorPayload : `Microservice error on pattern: ${pattern}`,
                  HttpStatus.BAD_GATEWAY,
                ),
            );
          }),
        ),
      );

      // Map microservice response to HTTP response
      return this.mapMicroserviceResponse(response);
    } catch (error) {
      // If the error is already an HttpException, re-throw it
      if (error instanceof HttpException) {
        throw error;
      }

      this.logger.error(`Unexpected error for pattern: ${pattern}`, error);
      throw new HttpException(
        'Internal gateway error',
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }
  /**
   * Maps microservice API response format to HTTP response.
   * Handles both success and error responses from the microservice.
   */
  private mapMicroserviceResponse<T>(response: any): T {
    this.logger.debug('Microservice response received:', response);

    // If response has success flag, it's formatted by ResponseHelper
    if (response && typeof response === 'object' && 'success' in response) {
      // Success response
      if (response.success === true) {
        this.logger.debug('✅ Success response from microservice');
        return response.data;
      }

      // Error response from microservice
      if (response.success === false) {
        this.logger.warn(
          `⚠️ Error response from microservice: ${response.message}`,
        );
        throw new HttpException(
          {
            message: response.message,
            errors: response.errors,
          },
          response.statusCode || HttpStatus.INTERNAL_SERVER_ERROR,
        );
      }
    }

    // Fallback for non-formatted responses (backward compatibility)
    this.logger.warn('Unformatted response from microservice:', response);
    return response;
  }
}