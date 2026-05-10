import { Controller, Post, Get, Body, Param, Put, Delete, ParseUUIDPipe } from '@nestjs/common';
import { KafkaService } from '../../../common/services/kafka.services';
import { CreateUserDto } from '../dtos/create-user.dto';

@Controller('users')
export class UsersController {
  constructor(private readonly kafkaService: KafkaService) {}

  @Post()
  async create(@Body() body: CreateUserDto) {
    return await this.kafkaService.send('user.create', body);
  }

  @Get()
  async findAll() {
    return await this.kafkaService.send('user.find.all', {});
  }

  @Get(':id')

  async findOne(@Param('id', ParseUUIDPipe) id: string) {
    return await this.kafkaService.send('user.find.one', { id });
  }

  @Put(':id')
  async update(@Param('id', ParseUUIDPipe) id: string, @Body() body: any) {
    return await this.kafkaService.send('user.update', { id, ...body });
  }

  @Delete(':id')
  async remove(@Param('id') id: string) {
    return await this.kafkaService.send('user.delete', { id });
  }
}
