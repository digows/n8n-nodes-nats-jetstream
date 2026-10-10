import type { ConsumerMessages } from '@nats-io/jetstream';
import { NodeConnectionTypes, NodeOperationError, type INodeType, type INodeTypeDescription, type ITriggerFunctions, type ITriggerResponse } from 'n8n-workflow';
import { bindConsumer, creationConfiguration, progressInterval } from '../../lib/consumer';
import { openConnection, optionalText, positiveInteger, requiredText, withinTimeout } from '../../lib/connection';
import { deliverMessage } from '../../lib/delivery';
import type { PayloadFormat } from '../../lib/payload';
import { NatsJetstream } from './NatsJetstream.node';

export class NatsJetstreamTrigger implements INodeType
{
    description: INodeTypeDescription = {
        displayName: 'NATS JetStream Trigger', name: 'natsJetstreamTrigger', group: ['trigger'], version: 1,
        description: 'Consume a durable pull consumer; ACK after successful execution or persisted Wait', subtitle: 'Consume a durable pull consumer',
        defaults: { name: 'NATS JetStream Trigger' }, icon: { light: 'file:jetstream.svg', dark: 'file:jetstream.svg' }, inputs: [], outputs: [NodeConnectionTypes.Main],
        credentials: [{ name: 'natsJetstreamApi', required: true, testedBy: 'testConnection' }],
        properties: [
            { displayName: 'Manual testing consumes real messages and can advance the selected durable consumer. Use a dedicated test consumer.', name: 'manualNotice', type: 'notice', default: '' },
            { displayName: 'Stream', name: 'stream', type: 'string', default: '', required: true },
            { displayName: 'Durable Consumer', name: 'durable', type: 'string', default: '', required: true },
            { displayName: 'Payload Format', name: 'payloadFormat', type: 'options', default: 'json', options: [{ name: 'JSON', value: 'json' }, { name: 'Text', value: 'text' }, { name: 'Binary', value: 'binary' }] },
            { displayName: 'Consumer Mode', name: 'consumerMode', type: 'options', default: 'existing', options: [{ name: 'Bind Existing', value: 'existing' }, { name: 'Create If Missing', value: 'create' }] },
            { displayName: 'Creation Settings', name: 'creation', type: 'collection', placeholder: 'Add Setting', default: {}, displayOptions: { show: { consumerMode: ['create'] } }, options: [
                { displayName: 'Ack Wait (Ms)', name: 'ackWaitMs', type: 'number', default: 30000, typeOptions: { minValue: 1 } },
                { displayName: 'Delivery Policy', name: 'deliverPolicy', type: 'options', default: 'new', options: [{ name: 'New', value: 'new' }, { name: 'All', value: 'all' }] },
                { displayName: 'Max Ack Pending', name: 'maxAckPending', type: 'number', default: 1000, typeOptions: { minValue: 1 } },
                { displayName: 'Max Deliver', name: 'maxDeliver', type: 'number', default: 10, description: 'Maximum delivery attempts; -1 means unlimited' },
                { displayName: 'Subject Filters', name: 'subjectFilters', type: 'string', default: '', description: 'Comma- or whitespace-separated subject filters; empty selects all subjects in the stream' },
            ] },
            { displayName: 'Options', name: 'options', type: 'collection', placeholder: 'Add Option', default: {}, options: [
                { displayName: 'ACK Confirmation Timeout (Ms)', name: 'ackTimeoutMs', type: 'number', default: 5000, typeOptions: { minValue: 1 } },
                { displayName: 'Delayed NAK Interval (Ms)', name: 'nakDelayMs', type: 'number', default: 5000, typeOptions: { minValue: 1 } },
                { displayName: 'Maximum In-Flight Messages', name: 'maxInFlight', type: 'number', default: 1, typeOptions: { minValue: 1 }, description: 'Includes fetched, queued, running and unconfirmed messages' },
                { displayName: 'Pull Timeout (Ms)', name: 'pullTimeoutMs', type: 'number', default: 5000, typeOptions: { minValue: 1000 } },
                { displayName: 'Shutdown Grace Period (Ms)', name: 'shutdownGraceMs', type: 'number', default: 30000, typeOptions: { minValue: 1 }, description: 'Automatic: wait for in-flight work on shutdown. Manual: settlement deadline from trigger output; expiry releases the message for redelivery while downstream execution may continue.' },
            ] },
        ],
    };

    methods = new NatsJetstream().methods;

    async trigger(this: ITriggerFunctions): Promise<ITriggerResponse>
    {
        const credentials = await this.getCredentials('natsJetstreamApi');
        const stream = requiredText(this.getNodeParameter('stream'), 'Stream');
        const durable = requiredText(this.getNodeParameter('durable'), 'Durable consumer');
        const mode = this.getNodeParameter('consumerMode') as 'existing' | 'create';
        if (!['existing', 'create'].includes(mode)) throw new NodeOperationError(this.getNode(), 'Select a supported consumer mode');
        const options = this.getNodeParameter('options') as Record<string, unknown>;
        const manual = this.getMode() === 'manual';
        const capacity = manual ? 1 : positiveInteger(options.maxInFlight ?? 1, 'Maximum in-flight messages');
        const pullTimeoutMs = positiveInteger(options.pullTimeoutMs ?? 5000, 'Pull timeout');
        if (pullTimeoutMs < 1000) throw new NodeOperationError(this.getNode(), 'Pull timeout must be at least 1000 ms');
        const shutdownGraceMs = positiveInteger(options.shutdownGraceMs ?? 30000, 'Shutdown grace period');
        const deliveryOptions = {
            format: this.getNodeParameter('payloadFormat') as PayloadFormat,
            nakDelayMs: positiveInteger(options.nakDelayMs ?? 5000, 'Delayed NAK interval'),
            ackTimeoutMs: positiveInteger(options.ackTimeoutMs ?? 5000, 'ACK confirmation timeout'),
            progressIntervalMs: 0,
        };
        const connection = await openConnection(credentials);
        try
        {
            const { consumer, info } = await bindConsumer(connection, {
                stream, durable, mode, domain: optionalText(credentials.domain),
                configuration: mode === 'create' ? creationConfiguration(durable, this.getNodeParameter('creation') as Record<string, unknown>) : undefined,
            });
            deliveryOptions.progressIntervalMs = progressInterval(info);
            const controller = new AbortController();
            const inFlight = new Set<Promise<void>>();
            let stopping = false;
            let connected = true;
            let iterator: ConsumerMessages | undefined;
            let loop: Promise<void> | undefined;
            let closing: Promise<void> | undefined;
            let listenerClosed: Promise<void> | undefined;
            const close = (): Promise<void> =>
            {
                if (closing) return manual ? listenerClosed! : closing;
                stopping = true;
                listenerClosed = (async () => { await iterator?.close(); })();
                closing = (async () =>
                {
                    await listenerClosed;
                    try { await withinTimeout(Promise.all([...inFlight]), shutdownGraceMs); }
                    catch
                    {
                        if (manual) this.logger.warn('NATS manual settlement exceeded the shutdown grace period; unfinished delivery released without success ACK');
                        controller.abort();
                    }
                    controller.abort();
                    await connection.close();
                    await loop;
                    await Promise.all([...inFlight]);
                })();
                if (manual)
                {
                    // The host awaits close before resolving its manual donePromise.
                    // Release that wait, but keep only the emitted delivery within the finite grace.
                    void closing.catch(() => this.logger.warn('NATS manual cleanup did not complete'));
                    return listenerClosed;
                }
                return closing;
            };
            const fatal = () =>
            {
                if (!stopping)
                {
                    this.emitError(new NodeOperationError(this.getNode(), 'NATS trigger cannot continue; check broker availability, consumer configuration and permissions'));
                    void close().catch(() => this.logger.warn('NATS trigger cleanup did not complete'));
                }
            };
            void (async () =>
            {
                try
                {
                    for await (const status of connection.status())
                    {
                        if (status.type === 'disconnect')
                        {
                            connected = false;
                            this.logger.warn('NATS disconnected; unconfirmed messages remain eligible for redelivery');
                        }
                        if (status.type === 'reconnect')
                        {
                            connected = true;
                            this.logger.info('NATS reconnected');
                        }
                    }
                }
                catch { fatal(); }
            })();
            void connection.closed().then(() =>
            {
                connected = false;
                controller.abort();
                fatal();
            });
            const listen = async () =>
            {
                try
                {
                    while (!stopping && !controller.signal.aborted)
                    {
                        if (inFlight.size >= capacity)
                        {
                            await Promise.race(inFlight);
                            continue;
                        }
                        if (!connected)
                        {
                            await new Promise(resolve => setTimeout(resolve, 100));
                            continue;
                        }
                        // fetch() is a bounded pull, unlike consume()'s automatic refill.
                        const available = capacity - inFlight.size;
                        try
                        {
                            iterator = await consumer.fetch({ max_messages: available, expires: pullTimeoutMs });
                            if (stopping) { await iterator.close(); break; }
                            for await (const message of iterator)
                            {
                                if (stopping || controller.signal.aborted) break;
                                const task = deliverMessage(this, message, deliveryOptions, controller.signal, () => connected && !connection.isClosed())
                                    .catch(() => fatal());
                                inFlight.add(task);
                                void task.finally(() => inFlight.delete(task));
                                if (manual) break;
                            }
                        }
                        catch (error)
                        {
                            // A bounded SDK fetch ends with this status during graceful broker restart.
                            const restarting = error instanceof Error && error.name === 'JetStreamStatusError' && error.message === 'server shutdown';
                            if (!restarting && connected) throw new NodeOperationError(this.getNode(), 'NATS pull failed; check consumer configuration and permissions');
                            this.logger.warn('NATS pull interrupted by disconnect; waiting to reconnect');
                            await new Promise(resolve => setTimeout(resolve, 100));
                        }
                        finally
                        {
                            await iterator?.close();
                            iterator = undefined;
                        }
                        if (manual && inFlight.size) break;
                    }
                }
                catch { fatal(); }
            };
            const start = () =>
            {
                if (!loop) loop = listen();
                return Promise.resolve();
            };
            if (!manual) await start();
            return { closeFunction: close, ...(manual ? { manualTriggerFunction: start } : {}) };
        }
        catch (error)
        {
            await connection.close();
            throw new NodeOperationError(this.getNode(), error instanceof Error ? error : new Error('Consumer activation failed'));
        }
    }
}
