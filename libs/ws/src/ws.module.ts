import { DynamicModule, Module } from '@nestjs/common';

import { createSecuredGateway } from './gateway/create-secured-gateway';
import { WsActionRegistry } from './registry/ws-action.registry';
import { EventPublisher } from './service/event-publisher';
import { WsPubSubBridge } from './service/ws-pubsub-bridge';
import { WsAction } from './type/ws-action.interface';
import { WsModuleOptions } from './type/ws-module.interface';

@Module({})
export class WsModule {
    static forRoot(): DynamicModule {
        return {
            module: WsModule,
            global: true,
            providers: [EventPublisher, WsPubSubBridge],
            exports: [EventPublisher],
        };
    }

    static forFeature(options: WsModuleOptions): DynamicModule {
        const GatewayClass = createSecuredGateway({
            namespace: options.namespace,
            connectionPermission: options.connectionPermission,
            cors: options.cors,
        });

        return {
            module: WsModule,
            imports: [...(options.imports ?? [])],
            providers: [
                GatewayClass,
                ...(options.actions ?? []),
                ...(options.providers ?? []),
                {
                    provide: WsActionRegistry,
                    useFactory: (...acts: WsAction[]) => new WsActionRegistry(acts),
                    inject: options.actions,
                },
            ],
        };
    }
}
