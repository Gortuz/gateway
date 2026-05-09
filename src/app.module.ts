import { Module } from '@nestjs/common';
import { UsersModule } from './users/users.module';

import { ConfigModule } from '@nestjs/config';
import { KafkaService } from './common/services/kafka.services';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    UsersModule,
  ],
  controllers: [],
  providers: [KafkaService],
  exports: [KafkaService],
})
export class AppModule {}
