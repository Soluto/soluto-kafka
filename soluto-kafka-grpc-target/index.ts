import {ProtobufMessage, Server, loadPackageDefinition, ServerCredentials, credentials} from 'grpc';
import {loadSync} from '@grpc/proto-loader';

const PROTO_PATH = __dirname + '/message.proto';

export type Headers = {
    recordOffset: number;
    recordTimestamp: number;
    topic: string;
    recordHeaders: string;
};

const packageDefinition = loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
});
const ProtobufMessage = loadPackageDefinition(packageDefinition) as ProtobufMessage;

const _callTarget = (run: any) => async (call: any, callback: any) => {
    try {
        const receivedTimestamp = Date.now();
        const payload = JSON.parse(call.request.msgJson);
        await run({
            payload,
            headers: {
                recordOffset: parseInt(call.request.recordOffset) || -1,
                recordTimestamp: parseInt(call.request.recordTimestamp) || -1,
                topic: call.request.topic,
                recordHeaders: call.request.headersJson ? JSON.parse(call.request.headersJson) : undefined,
            },
        });
        callback(null, {statusCode: 200, receivedTimestamp, completedTimestamp: Date.now()});
    } catch (e) {
        callback(null, {statusCode: e.statusCode ?? e.status ?? 500});
    }
};

const getServer = (execute: any) => {
    const server = new Server();
    server.addService(ProtobufMessage.CallTarget.service, {
        callTarget: _callTarget(execute),
    });
    return server;
};

export const startServer = (port: string, execute: any) => {
    const routeServer = getServer(execute);
    routeServer.bind(`0.0.0.0:${port}`, ServerCredentials.createInsecure());
    routeServer.start();
    return routeServer;
};

type TargetResponse = {
    statusCode: number;
};

let _clients: {[key: string]: any} = {};
export const createClient = (url: string) => {
    if (!_clients[url]) {
        _clients[url] = new ProtobufMessage.CallTarget(url, credentials.createInsecure());
    }
    return {
        callTarget: <T>(
            payload: T,
            recordOffset?: number,
            topic?: string,
            headersJson?: string
        ): Promise<TargetResponse> =>
            new Promise((resolve) =>
                _clients[url].callTarget(
                    {msgJson: JSON.stringify(payload), recordOffset, topic, headersJson},
                    (_: any, responsePayload: TargetResponse) => resolve(responsePayload)
                )
            ),
    };
};
