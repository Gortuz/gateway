import { Module } from '@nestjs/common';

import { UsersController } from './infrastrucutre/controllers/users.controller';
import { KafkaService } from '../common/services/kafka.services';

@Module({
  controllers: [UsersController],
  providers: [KafkaService],
})
export class UsersModule {}
