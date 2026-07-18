import { Injectable } from '@nestjs/common';
import { CreateRequestContext, MikroORM } from '@mikro-orm/core';
import { Processor } from '@nestjs/bullmq';
import { Job } from 'bullmq';

import { AbstractProcessor } from '@libs/core';
import { ChatEntity, MatchEntity, MatchRequestEntity, MatchRequestStatus, MatchStatus } from '@libs/orm';
import { EventPublisher } from '@libs/ws';

import { CHAT_READY_TIMEOUT_QUEUE } from '../constant/queue.constant';
import { ChatReadyTimeoutJobData } from '../dto/job-data/chat-ready-timeout.job-data';

@Processor(CHAT_READY_TIMEOUT_QUEUE)
@Injectable()
export class ChatReadyTimeoutProcessor extends AbstractProcessor<ChatReadyTimeoutJobData, void> {
    constructor(
        private readonly orm: MikroORM,
        private readonly events: EventPublisher,
    ) {
        super();
    }

    @CreateRequestContext()
    async process(job: Job<ChatReadyTimeoutJobData>): Promise<void> {
        const { matchId, userIds } = job.data;

        await this.orm.em.findOneOrFail(ChatEntity, { match: { id: matchId } });

        const match = await this.orm.em.findOneOrFail(MatchEntity, { id: matchId, status: MatchStatus.ACCEPTED });
        match.status = MatchStatus.CANCELLED;

        const matchRequests = await this.orm.em.find(MatchRequestEntity, {
            match: this.orm.em.getReference(MatchEntity, match.id),
        });
        for (const request of matchRequests) {
            request.status = MatchRequestStatus.CANCELLED;
        }

        await this.orm.em.flush();

        await Promise.all(
            userIds.map((userId) =>
                this.events.publishToUser({
                    userId,
                    event: 'search:error',
                    payload: { message: 'Chat creation timeout' },
                }),
            ),
        );
    }
}
