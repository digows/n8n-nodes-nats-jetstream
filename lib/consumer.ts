import { AckPolicy, DeliverPolicy, JetStreamApiError, JetStreamApiCodes, jetstream, jetstreamManager, type Consumer, type ConsumerConfig, type ConsumerInfo } from '@nats-io/jetstream';
import type { NatsConnection } from '@nats-io/transport-node';
import { positiveInteger, requiredText } from './connection';

export interface ConsumerSelection
{
    stream: string;
    durable: string;
    domain?: string;
    mode: 'existing' | 'create';
    configuration?: Partial<ConsumerConfig>;
}

export function creationConfiguration(durable: string, options: Record<string, unknown>): Partial<ConsumerConfig>
{
    const filters = typeof options.subjectFilters === 'string' ? options.subjectFilters.split(/[\s,]+/).filter(Boolean) : [];
    const maxDeliver = options.maxDeliver ?? 10;
    if (maxDeliver !== -1) positiveInteger(maxDeliver, 'Max deliver');
    const maxAckPending = options.maxAckPending ?? 1000;
    if (maxAckPending !== -1) positiveInteger(maxAckPending, 'Max ack pending');
    const ackWaitMs = positiveInteger(options.ackWaitMs ?? 30000, 'Ack wait');
    if (!Number.isSafeInteger(ackWaitMs * 1000000)) throw new Error('Ack wait exceeds the supported duration');
    return {
        durable_name: durable,
        ack_policy: AckPolicy.Explicit,
        deliver_policy: options.deliverPolicy === 'all' ? DeliverPolicy.All : DeliverPolicy.New,
        ack_wait: ackWaitMs * 1000000,
        max_deliver: maxDeliver as number,
        max_ack_pending: maxAckPending as number,
        ...(filters.length === 1 ? { filter_subject: filters[0] } : filters.length > 1 ? { filter_subjects: filters } : {}),
    };
}

export function validateConsumer(info: ConsumerInfo, selection: ConsumerSelection): void
{
    const configuration = info.config;
    if (configuration.durable_name !== selection.durable || configuration.deliver_subject || configuration.ack_policy !== AckPolicy.Explicit)
    {
        throw new Error('Select a named durable pull consumer with AckExplicit; push, ephemeral and other ACK policies are unsupported');
    }
    if (selection.mode === 'create')
    {
        const requested = selection.configuration!;
        const filters = (value: Partial<ConsumerConfig>) => [...(value.filter_subjects ?? (value.filter_subject ? [value.filter_subject] : []))].sort();
        const differences = ['deliver_policy', 'ack_wait', 'max_deliver', 'max_ack_pending'].filter(field => configuration[field as keyof ConsumerConfig] !== requested[field as keyof ConsumerConfig]);
        if (JSON.stringify(filters(configuration)) !== JSON.stringify(filters(requested))) differences.push('subject filters');
        if (configuration.backoff?.length) differences.push('backoff');
        if (differences.length) throw new Error(`Existing consumer configuration differs: ${differences.join(', ')}. It was not modified`);
    }
}

export async function bindConsumer(connection: NatsConnection, selection: ConsumerSelection): Promise<{ consumer: Consumer; info: ConsumerInfo }>
{
    requiredText(selection.stream, 'Stream');
    requiredText(selection.durable, 'Durable consumer');
    const manager = await jetstreamManager(connection, { domain: selection.domain, checkAPI: false });
    let info: ConsumerInfo;
    try
    {
        info = await manager.consumers.info(selection.stream, selection.durable);
    }
    catch (error)
    {
        if (!(error instanceof JetStreamApiError) || error.code !== JetStreamApiCodes.ConsumerNotFound || selection.mode !== 'create') throw error;
        const version = /^(\d+)\.(\d+)\./.exec(connection.info?.version ?? '');
        if (!version || Number(version[1]) < 2 || (Number(version[1]) === 2 && Number(version[2]) < 10))
        {
            throw new Error('Creating a consumer requires nats-server 2.10 or newer for create-only safety; use Bind Existing on older servers');
        }
        try
        {
            // SDK 3.4.0 add() uses action=create, never create-or-update.
            info = await manager.consumers.add(selection.stream, selection.configuration!);
        }
        catch (creationError)
        {
            // nats-server reports a create-only race as API error 10148 (HTTP-style status 400).
            if (!(creationError instanceof JetStreamApiError) || creationError.code !== 10148) throw creationError;
            info = await manager.consumers.info(selection.stream, selection.durable);
        }
    }
    validateConsumer(info, selection);
    const consumer = await jetstream(connection, { domain: selection.domain }).consumers.get(selection.stream, selection.durable);
    return { consumer, info };
}

export function progressInterval(info: ConsumerInfo): number
{
    const deadlines = info.config.backoff?.length ? info.config.backoff : [info.config.ack_wait];
    const milliseconds = Math.min(...deadlines.map(value => Number(value) / 1000000));
    if (!Number.isFinite(milliseconds) || milliseconds <= 0) throw new Error('Consumer must have a positive effective acknowledgement deadline');
    return Math.max(1, Math.floor(milliseconds / 3));
}
