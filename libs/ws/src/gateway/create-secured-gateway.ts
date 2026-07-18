import { ForbiddenException, Injectable, Type, UnauthorizedException } from '@nestjs/common';
import { ModuleRef } from '@nestjs/core';
import {
    OnGatewayConnection,
    OnGatewayDisconnect,
    OnGatewayInit,
    WebSocketGateway,
    WebSocketServer,
} from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';

import { SocketRegistry, WsServerRegistry } from '@libs/core';
import { SecurityManager, WsSecurityGuard } from '@libs/security';

import { BROADCAST_CHANNEL, userChannel } from '../constant/ws-channel.constant';
import { getWsActionGuards } from '../decorator/use-ws-guards.decorator';
import { WsActionRegistry } from '../registry/ws-action.registry';
import { AuthenticatedSocket } from '../type/authenticated-socket.type';
import { WsActionGuard } from '../type/ws-action-guard.interface';

export interface WsGatewayOptions {
    namespace: string;
    connectionPermission?: string;
    cors?: object;
}

export function createSecuredGateway(options: WsGatewayOptions): Type<any> {
    @WebSocketGateway({
        namespace: options.namespace,
        cors: options.cors ?? { origin: '*' },
    })
    @Injectable()
    class DynamicSecuredGateway implements OnGatewayInit, OnGatewayConnection, OnGatewayDisconnect {
        @WebSocketServer()
        private readonly server: Server;

        constructor(
            private readonly wsActionRegistry: WsActionRegistry,
            private readonly socketRegistry: SocketRegistry,
            private readonly wsServerRegistry: WsServerRegistry,
            private readonly guard: WsSecurityGuard,
            private readonly securityManager: SecurityManager,
            private readonly moduleRef: ModuleRef,
        ) {}

        afterInit(server: Server): void {
            this.wsServerRegistry.set(options.namespace, server);

            server.use(async (client: Socket & { data: any }, next) => {
                const user = this.guard.authenticate(client);
                if (!user) return next(new UnauthorizedException());
                client.data.user = user;

                if (options.connectionPermission) {
                    const granted = await this.securityManager.isGranted(options.connectionPermission, user.sub);
                    if (!granted) return next(new ForbiddenException());
                }

                next();
            });
        }

        handleConnection(client: AuthenticatedSocket): void {
            const userId = client.data.user.sub;
            client.join(userChannel(userId));
            client.join(BROADCAST_CHANNEL);
            this.socketRegistry.of(options.namespace).set(userId, client);

            client.onAny(async (event: string, data: unknown) => {
                const action = this.wsActionRegistry.get(event);
                if (!action) {
                    client.emit('error', { message: `Unknown event: ${event}` });
                    return;
                }

                try {
                    const guardClasses = getWsActionGuards(action.invoke);
                    for (const GuardClass of guardClasses) {
                        const guardInstance = this.moduleRef.get<WsActionGuard>(GuardClass, { strict: false });
                        await guardInstance.canActivate(client, data);
                    }

                    await action.invoke(client, data, this.server);
                } catch (err) {
                    client.emit('error', { message: err.message });
                }
            });
        }

        handleDisconnect(client: AuthenticatedSocket): void {
            this.socketRegistry.of(options.namespace).remove(client.data.user.sub);
        }
    }

    return DynamicSecuredGateway;
}
