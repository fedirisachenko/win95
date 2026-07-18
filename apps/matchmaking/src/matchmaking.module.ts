import 'dotenv/config';
import { Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { BullModule } from '@nestjs/bullmq';
import { BullBoardModule } from '@bull-board/nestjs';
import { ExpressAdapter } from '@bull-board/express';
import { OrmModule } from '@libs/orm';
import { CoreModule } from '@libs/core';
import { SecurityModule } from '@libs/security';
import { RmqModule } from '@libs/rmq';
import { WsModule } from '@libs/ws';
import { RedisModule } from '@songkeys/nestjs-redis';
import mikroOrmConfig from '@config/mikro-orm.config';
import redisConfig from '@config/redis.config';
import bullmqConfig from '@config/bullmq.config';
import { SearchModule } from './search/search.module';
import { MatchModule } from './match/match.module';

@Module({
    imports: [
        OrmModule.register(mikroOrmConfig),
        CoreModule.register(),
        SecurityModule.forRoot(),
        RedisModule.forRoot(redisConfig),
        WsModule.forRoot(),
        BullModule.forRoot(bullmqConfig),
        BullBoardModule.forRoot({
            route: '/queues',
            adapter: ExpressAdapter,
        }),
        RmqModule.forRootAsync({
            useFactory: (configService: ConfigService) => ({
                urls: configService.get<string>('AMQP_URLS', 'amqp://localhost:56721').split(','),
                exchange: configService.get<string>('AMQP_EXCHANGE_NAME', 'broadcast'),
                exchangeType: 'direct',
            }),
            inject: [ConfigService],
        }),
        SearchModule,
        MatchModule,
    ],
})
export class MatchmakingModule {}
