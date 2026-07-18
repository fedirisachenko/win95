import { Injectable } from '@nestjs/common';

import { EventPublisher } from '@libs/ws';

import { ChatReadyInput } from '../transport/rmq/dto/input/chat-ready.input';

@Injectable()
export class ChatReadyActionService {
    constructor(private readonly events: EventPublisher) {}

    async invoke(data: ChatReadyInput): Promise<void> {
        await Promise.all(
            data.userIds.map((userId) =>
                this.events.publishToUser({
                    userId,
                    event: 'search:completed',
                    payload: { chatId: data.chatId },
                }),
            ),
        );
    }
}
