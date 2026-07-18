import { Injectable } from '@nestjs/common';
import { CreateRequestContext, MikroORM } from '@mikro-orm/core';
import { Processor } from '@nestjs/bullmq';
import { RedisService } from '@songkeys/nestjs-redis';
import { Job } from 'bullmq';

import { AbstractProcessor } from '@libs/core';
import { MatchEntity, MatchRequestEntity, MatchRequestStatus, MatchStatus } from '@libs/orm';
import { EventPublisher } from '@libs/ws';

import { RedisKey } from '../../constant/redis-key.constant';
import { ACCEPT_TIMEOUT_QUEUE } from '../constant/queue.constant';
import { AcceptTimeoutJobData } from '../dto/job-data/accept-timeout.job-data';

@Processor(ACCEPT_TIMEOUT_QUEUE)
@Injectable()
export class AcceptTimeoutProcessor extends AbstractProcessor<AcceptTimeoutJobData, void> {
    constructor(
        private readonly orm: MikroORM,
        private readonly redis: RedisService,
        private readonly events: EventPublisher,
    ) {
        super();
    }

    @CreateRequestContext()
    async process(job: Job<AcceptTimeoutJobData>): Promise<void> {
        const { matchId, userIds } = job.data;
        const client = this.redis.getClient();
        const acceptKey = RedisKey.matchmakingAccept(matchId);

        const acceptedUserCount = Number(await client.get(acceptKey)) || 0;
        if (acceptedUserCount >= 2) return;

        await client.del(acceptKey);

        const match = await this.orm.em.findOneOrFail(MatchEntity, { id: matchId });
        if (match.status === MatchStatus.PENDING) {
            match.status = MatchStatus.CANCELLED;
        }

        const matchRequests = await this.orm.em.find(MatchRequestEntity, {
            match: this.orm.em.getReference(MatchEntity, match.id),
        });
        for (const request of matchRequests) {
            request.status = MatchRequestStatus.CANCELLED;
        }

        await this.orm.em.flush();

        await Promise.all(
            userIds.map((userId) => this.events.publishToUser({ userId, event: 'search:timeout', payload: {} })),
        );
    }
}
