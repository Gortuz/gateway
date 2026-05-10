import { IsUUID } from 'class-validator';

export class FindByIdUserDto {
  @IsUUID("all", { message: 'ID must be an UUID' })
  id: string
}
