import { Injectable } from '@nestjs/common';
import { CreateRequestContext, MikroORM, ref } from '@mikro-orm/core';
import { InjectQueue, Processor } from '@nestjs/bullmq';
import { RedisService } from '@songkeys/nestjs-redis';
import { Job, Queue } from 'bullmq';

import { AbstractProcessor } from '@libs/core';
import { MatchEntity, MatchRequestEntity, MatchStatus } from '@libs/orm';
import { EventPublisher } from '@libs/ws';

import { ACCEPT_TIMEOUT_SECONDS } from '../../constant/matchmaking.constant';
import { RedisKey } from '../../constant/redis-key.constant';
import { MATCH_LUA } from '../constant/lua.constant';
import { ACCEPT_TIMEOUT_QUEUE, MATCH_ATTEMPT_QUEUE } from '../constant/queue.constant';
import { AcceptTimeoutJobData } from '../dto/job-data/accept-timeout.job-data';
import { MatchAttemptJobData } from '../dto/job-data/match-attempt.job-data';
import { MatchmakingService } from '../service/matchmaking.service';

@Processor(MATCH_ATTEMPT_QUEUE)
@Injectable()
export class MatchAttemptProcessor extends AbstractProcessor<MatchAttemptJobData, void> {
    constructor(
        private readonly orm: MikroORM,
        private readonly redis: RedisService,
        private readonly events: EventPublisher,
        private readonly matchmakingService: MatchmakingService,
        @InjectQueue(MATCH_ATTEMPT_QUEUE) private readonly matchAttemptQueue: Queue,
        @InjectQueue(ACCEPT_TIMEOUT_QUEUE) private readonly acceptTimeoutQueue: Queue,
    ) {
        super();
    }

    async process(job: Job<MatchAttemptJobData>): Promise<void> {
        const { duration, language } = job.data;
        const client = this.redis.getClient();
        const key = RedisKey.matchAttemptQueue(duration, language);

        const result = await client.eval(MATCH_LUA, 1, key);
        if (result) await this.onMatch(result as string[]);

        const remaining = await client.zcard(key);
        if (remaining < 2) return;

        await this.matchAttemptQueue.add(
            'match-attempt',
            { duration, language },
            { jobId: `mm:${duration}:${language}`, delay: 100 },
        );
    }

    @CreateRequestContext()
    private async onMatch(userIds: string[]): Promise<void> {
        const client = this.redis.getClient();

        const userKeys = userIds.map((userId) => RedisKey.matchmakingUser(userId));
        const rawSearchData = await client.mget(...userKeys);

        await Promise.all(userIds.map((userId) => this.matchmakingService.dequeue(userId)));

        if (!rawSearchData.length) return;

        const parsedSearchData = rawSearchData.map((data) => JSON.parse(data));
        const matchRequests = await this.orm.em.find(
            MatchRequestEntity,
            { id: { $in: parsedSearchData.map((data) => data.searchId) } },
            { populate: ['user'] },
        );

        let matchId: string;
        await this.orm.em.transactional(async (em) => {
            const match = em.create(MatchEntity, { status: MatchStatus.PENDING });
            matchId = match.id;

            for (const request of matchRequests) {
                request.match = ref(match);
            }
            await em.flush();
        });

        const acceptTimeoutJobData: AcceptTimeoutJobData = { matchId, userIds };
        await this.acceptTimeoutQueue.add('accept-timeout', acceptTimeoutJobData, {
            delay: ACCEPT_TIMEOUT_SECONDS * 1000,
        });

        await Promise.all(
            matchRequests.map((request) =>
                this.events.publishToUser({
                    userId: request.user.id,
                    event: 'search:found',
                    payload: { searchId: request.id, acceptTime: ACCEPT_TIMEOUT_SECONDS },
                }),
            ),
        );
    }
}
