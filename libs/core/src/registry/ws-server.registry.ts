import { Injectable } from '@nestjs/common';
import { Server } from 'socket.io';

@Injectable()
export class WsServerRegistry {
    private readonly servers = new Map<string, Server>();

    set(namespace: string, server: Server): void {
        this.servers.set(namespace, server);
    }

    of(namespace: string): Server | undefined {
        return this.servers.get(namespace);
    }

    all(): Server[] {
        return Array.from(this.servers.values());
    }
}
