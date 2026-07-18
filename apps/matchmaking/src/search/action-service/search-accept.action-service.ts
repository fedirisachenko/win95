import { Injectable } from '@nestjs/common';
import { CreateRequestContext, MikroORM } from '@mikro-orm/core';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';
import { RedisService } from '@songkeys/nestjs-redis';

import { MatchRequestEntity, MatchStatus } from '@libs/orm';
import { EventPublisher } from '@libs/ws';

import { ACCEPT_TTL_SECONDS, CHAT_READY_TIMEOUT_SECONDS } from '../../constant/matchmaking.constant';
import { RedisKey } from '../../constant/redis-key.constant';
import { CHAT_READY_TIMEOUT_QUEUE } from '../../match/constant/queue.constant';
import { ChatReadyTimeoutJobData } from '../../match/dto/job-data/chat-ready-timeout.job-data';
import { SearchAcceptInput } from '../transport/ws/dto/input/search-accept.input';

export type MatchAcceptedEvent = {
    matchId: string;
    userIds: string[];
    duration: number;
};

@Injectable()
export class SearchAcceptActionService {
    constructor(
        private readonly orm: MikroORM,
        private readonly redis: RedisService,
        private readonly events: EventPublisher,
        @InjectQueue(CHAT_READY_TIMEOUT_QUEUE) private readonly chatReadyTimeoutQueue: Queue,
    ) {}

    @CreateRequestContext()
    async invoke(userId: string, data: SearchAcceptInput): Promise<MatchAcceptedEvent | null> {
        const matchRequest = await this.orm.em.findOneOrFail(
            MatchRequestEntity,
            {
                id: data.searchId,
                user: { id: userId },
                match: { status: MatchStatus.PENDING },
            },
            { populate: ['match'] },
        );

        const match = matchRequest.match.unwrap();
        const acceptKey = RedisKey.matchmakingAccept(match.id);
        const client = this.redis.getClient();
        const acceptedUserCount = await client.incr(acceptKey);

        if (acceptedUserCount < 2) {
            await client.expire(acceptKey, ACCEPT_TTL_SECONDS);
            await this.events.publishToUser({ userId, event: 'search:waiting', payload: {} });
            return null;
        }

        const allRequests = await this.orm.em.find(
            MatchRequestEntity,
            { match: { id: match.id } },
            { populate: ['user'] },
        );

        match.status = MatchStatus.ACCEPTED;
        await this.orm.em.flush();

        await client.del(acceptKey);

        const userIds = allRequests.map((request) => request.user.id);

        const chatReadyTimeoutJobData: ChatReadyTimeoutJobData = {
            matchId: match.id,
            userIds,
        };

        await this.chatReadyTimeoutQueue.add('chat-ready-timeout', chatReadyTimeoutJobData, {
            delay: CHAT_READY_TIMEOUT_SECONDS * 1000,
        });

        return {
            matchId: match.id,
            userIds,
            duration: allRequests[0].desiredDuration,
        };
    }
}
