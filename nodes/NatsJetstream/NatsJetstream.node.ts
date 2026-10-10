import { jetstream } from '@nats-io/jetstream';
import type { NatsConnection } from '@nats-io/transport-node';
import { NodeConnectionTypes, NodeOperationError, type ICredentialTestFunctions, type ICredentialsDecrypted, type ICredentialDataDecryptedObject, type IExecuteFunctions, type INodeCredentialTestResult, type INodeExecutionData, type INodeType, type INodeTypeDescription } from 'n8n-workflow';
import { openConnection, optionalText, positiveInteger, withinTimeout } from '../../lib/connection';
import { publishHeaders, publishPayload, publishSubject, sequenceString, type PayloadFormat } from '../../lib/payload';

export class NatsJetstream implements INodeType
{
    description: INodeTypeDescription = {
        displayName: 'NATS JetStream', name: 'natsJetstream', group: ['output'], version: 1,
        subtitle: 'Publish a message', description: 'Publish to NATS JetStream and await broker acknowledgement',
        defaults: { name: 'NATS JetStream' }, icon: { light: 'file:jetstream.svg', dark: 'file:jetstream.svg' },
        inputs: [NodeConnectionTypes.Main], outputs: [NodeConnectionTypes.Main], usableAsTool: true,
        credentials: [{ name: 'natsJetstreamApi', required: true, testedBy: 'testConnection' }],
        properties: [
            { displayName: 'Subject', name: 'subject', type: 'string', default: '', required: true, placeholder: 'orders.created', description: 'Literal publication subject; supports expressions but not subscription wildcards' },
            { displayName: 'Payload Format', name: 'payloadFormat', type: 'options', default: 'json', options: [{ name: 'JSON', value: 'json' }, { name: 'Text', value: 'text' }, { name: 'Binary', value: 'binary' }] },
            { displayName: 'JSON Payload', name: 'jsonPayload', type: 'json', default: '={{ $json }}', displayOptions: { show: { payloadFormat: ['json'] } } },
            { displayName: 'Text Payload', name: 'textPayload', type: 'string', default: '', displayOptions: { show: { payloadFormat: ['text'] } } },
            { displayName: 'Input Binary Field', name: 'binaryProperty', type: 'string', default: 'data', displayOptions: { show: { payloadFormat: ['binary'] } } },
            { displayName: 'Options', name: 'options', type: 'collection', placeholder: 'Add Option', default: {}, options: [
                { displayName: 'Headers', name: 'headers', type: 'json', default: '{}', description: 'Object of strings or string arrays; arrays preserve repeated header values' },
                { displayName: 'Message ID', name: 'messageId', type: 'string', default: '', description: 'Stable ID reused when retrying the same logical publication' },
                { displayName: 'Expected Stream', name: 'expectedStream', type: 'string', default: '' },
                { displayName: 'Publish Timeout (Ms)', name: 'publishTimeoutMs', type: 'number', default: 10000, typeOptions: { minValue: 1 } },
            ] },
        ],
    };

    methods = {
        credentialTest: {
            async testConnection(this: ICredentialTestFunctions, credential: ICredentialsDecrypted<ICredentialDataDecryptedObject>): Promise<INodeCredentialTestResult>
            {
                if (!credential.data) return { status: 'Error', message: 'NATS credentials are missing' };
                let connection: NatsConnection | undefined;
                try
                {
                    connection = await openConnection(credential.data);
                    await withinTimeout(connection.flush(), positiveInteger(credential.data.connectionTimeoutMs ?? 10000, 'Connection timeout'));
                    return { status: 'OK', message: 'NATS connectivity and authentication succeeded; operation permissions were not tested' };
                }
                catch
                {
                    return { status: 'Error', message: 'NATS connectivity test failed' };
                }
                finally
                {
                    await connection?.close();
                }
            },
        },
    };

    async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]>
    {
        const credentials = await this.getCredentials('natsJetstreamApi');
        const connection = await openConnection(credentials);
        const results: INodeExecutionData[] = [];
        try
        {
            const client = jetstream(connection, { domain: optionalText(credentials.domain) });
            for (let itemIndex = 0; itemIndex < this.getInputData().length; itemIndex++)
            {
                try
                {
                    const subject = this.getNodeParameter('subject', itemIndex) as string;
                    publishSubject(subject);
                    const format = this.getNodeParameter('payloadFormat', itemIndex) as PayloadFormat;
                    const options = this.getNodeParameter('options', itemIndex) as Record<string, unknown>;
                    const messageId = optionalText(options.messageId);
                    const expectedStream = optionalText(options.expectedStream);
                    const headerValue = typeof options.headers === 'string' ? JSON.parse(options.headers) as unknown : options.headers ?? {};
                    const messageHeaders = publishHeaders(headerValue, messageId, expectedStream);
                    let data: Uint8Array;
                    if (format === 'binary')
                    {
                        data = await this.helpers.getBinaryDataBuffer(itemIndex, this.getNodeParameter('binaryProperty', itemIndex) as string);
                    }
                    else
                    {
                        let value: unknown = this.getNodeParameter(format === 'json' ? 'jsonPayload' : 'textPayload', itemIndex);
                        if (format === 'json' && typeof value === 'string') value = JSON.parse(value) as unknown;
                        data = publishPayload(value, format);
                    }
                    const acknowledgement = await client.publish(subject, data, {
                        headers: messageHeaders, msgID: messageId,
                        expect: expectedStream ? { streamName: expectedStream } : undefined,
                        timeout: positiveInteger(options.publishTimeoutMs ?? 10000, 'Publish timeout'),
                    });
                    results.push({ json: { subject, stream: acknowledgement.stream, sequence: sequenceString(acknowledgement.seq), duplicate: acknowledgement.duplicate, messageId: messageId ?? (messageHeaders.get('Nats-Msg-Id') || null) }, pairedItem: { item: itemIndex } });
                }
                catch (error)
                {
                    if (this.continueOnFail())
                    {
                        results.push({ json: { error: error instanceof Error ? error.message : 'JetStream publication failed' }, pairedItem: { item: itemIndex } });
                        continue;
                    }
                    throw new NodeOperationError(this.getNode(), error instanceof Error ? error : new Error('JetStream publication failed'), { itemIndex, description: 'A timeout is ambiguous: the broker may have stored the message. Reuse its stable message ID on retry' });
                }
            }
            return [results];
        }
        finally
        {
            await connection.close();
        }
    }
}
