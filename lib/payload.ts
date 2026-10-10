import { headers, type MsgHdrs } from '@nats-io/transport-node';
import type { JsMsg } from '@nats-io/jetstream';
import type { IDataObject, INodeExecutionData, ITriggerFunctions } from 'n8n-workflow';

export type PayloadFormat = 'json' | 'text' | 'binary';

export function sequenceString(sequence: number): string
{
    if (!Number.isSafeInteger(sequence) || sequence < 0) throw new Error('Broker sequence exceeds safe JavaScript integer precision');
    return String(sequence);
}

export function metadata(message: JsMsg): IDataObject
{
    const headerValues: IDataObject = Object.fromEntries(message.headers ? [...message.headers] : []);
    return {
        subject: message.subject,
        stream: message.info.stream,
        consumer: message.info.consumer,
        streamSequence: sequenceString(message.info.streamSequence),
        consumerSequence: sequenceString(message.info.deliverySequence),
        deliveryCount: message.info.deliveryCount,
        redelivered: message.redelivered,
        timestamp: message.timestamp,
        messageId: message.headers?.get('Nats-Msg-Id') || null,
        headers: headerValues,
    };
}

export async function messageItem(message: JsMsg, format: PayloadFormat, helpers: Pick<ITriggerFunctions['helpers'], 'prepareBinaryData'>): Promise<INodeExecutionData>
{
    const messageMetadata = metadata(message);
    if (format === 'binary')
    {
        return { json: { metadata: messageMetadata }, binary: { data: await helpers.prepareBinaryData(Buffer.from(message.data), 'message.bin', 'application/octet-stream') } };
    }
    const text = new TextDecoder('utf-8', { fatal: true }).decode(message.data);
    if (format === 'text') return { json: { payload: text, metadata: messageMetadata } };
    if (format !== 'json') throw new Error('Select JSON, Text or Binary payload format');
    // Parse errors deliberately omit payload contents from error messages.
    try
    {
        return { json: { payload: JSON.parse(text) as IDataObject, metadata: messageMetadata } };
    }
    catch
    {
        throw new Error('Message is not valid JSON');
    }
}

export function publishSubject(subject: string): void
{
    if (!subject || subject.split('.').some(token => !token || /[\s*>]/.test(token))) throw new Error('Publish subject must be a literal NATS subject without wildcards or whitespace');
}

export function publishHeaders(value: unknown, messageId?: string, expectedStream?: string): MsgHdrs
{
    if (typeof value !== 'object' || value === null || Array.isArray(value)) throw new Error('Headers must be an object of strings or string arrays');
    const result = headers();
    for (const [name, values] of Object.entries(value))
    {
        if (!name || /[\r\n:]/.test(name)) throw new Error('Invalid header name');
        const entries: unknown[] = Array.isArray(values) ? values : [values];
        for (const entry of entries)
        {
            if (typeof entry !== 'string' || /[\r\n]/.test(entry)) throw new Error('Header values must be strings without newlines');
            result.append(name, entry);
        }
    }
    const reservedHeaders: [string, string | undefined][] = [['Nats-Msg-Id', messageId], ['Nats-Expected-Stream', expectedStream]];
    for (const [name, dedicatedValue] of reservedHeaders)
    {
        const existing = result.values(name);
        if (dedicatedValue && existing.length && (existing.length !== 1 || existing[0] !== dedicatedValue)) throw new Error(`Dedicated parameter conflicts with ${name} header`);
    }
    return result;
}

export function publishPayload(value: unknown, format: Exclude<PayloadFormat, 'binary'>): Uint8Array
{
    if (format === 'text')
    {
        if (typeof value !== 'string') throw new Error('Text payload must be a string');
        return new TextEncoder().encode(value);
    }
    if (format !== 'json') throw new Error('Select JSON, Text or Binary payload format');
    const encoded = JSON.stringify(value);
    if (encoded === undefined) throw new Error('JSON payload is not serializable');
    return new TextEncoder().encode(encoded);
}
