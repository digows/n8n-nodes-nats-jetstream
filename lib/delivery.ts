import type { JsMsg } from '@nats-io/jetstream';
import type { IRun, ITriggerFunctions } from 'n8n-workflow';
import { NodeOperationError } from 'n8n-workflow';
import { messageItem, type PayloadFormat } from './payload';

export interface DeliveryOptions
{
    format: PayloadFormat;
    progressIntervalMs: number;
    nakDelayMs: number;
    ackTimeoutMs: number;
    onExecutionError?: 'redeliver' | 'acknowledge';
}

export function completedBoundary(result: IRun | undefined): boolean
{
    if (!result?.data?.resultData || result.data.resultData.error) return false;
    if (result.status === 'success') return true;
    return result.status === 'waiting' && Boolean(result.waitTill) && Number.isFinite(new Date(result.waitTill!).getTime());
}

export async function deliverMessage(context: ITriggerFunctions, message: JsMsg, options: DeliveryOptions, signal: AbortSignal, connected: () => boolean): Promise<void>
{
    await deliverBatch(context, [message], options, signal, connected);
}

export async function deliverBatch(context: ITriggerFunctions, source: Iterable<JsMsg> | AsyncIterable<JsMsg>, options: DeliveryOptions, signal: AbortSignal, connected: () => boolean, canEmit: () => boolean = () => true): Promise<void>
{
    const messages: JsMsg[] = [];
    const unsettled = new Set<JsMsg>();
    let abortHandler: (() => void) | undefined;
    const aborted = new Promise<undefined>(resolve =>
    {
        abortHandler = () => resolve(undefined);
        signal.addEventListener('abort', abortHandler, { once: true });
        if (signal.aborted) resolve(undefined);
    });
    const progress = () =>
    {
        if (connected()) for (const message of unsettled) message.working();
    };
    const release = (deliveries: JsMsg[] = messages) =>
    {
        if (connected()) for (const message of deliveries) message.nak(options.nakDelayMs);
    };
    const timer = setInterval(() =>
    {
        try { progress(); }
        catch { context.logger.warn('NATS delivery progress could not be sent'); }
    }, options.progressIntervalMs);
    try
    {
        try
        {
            for await (const message of source)
            {
                messages.push(message);
                unsettled.add(message);
                if (connected()) message.working();
                if (signal.aborted) break;
            }
        }
        catch (error)
        {
            release();
            throw error;
        }
        if (!messages.length) return;
        if (signal.aborted || !canEmit()) { release(); return; }
        const completion = context.helpers.createDeferredPromise<IRun>();
        const resultPromise = completion.promise.then(result => result, () => undefined);
        const items = [];
        try
        {
            for (const [index, message] of messages.entries())
            {
                const item = await messageItem(message, options.format, context.helpers);
                items.push(messages.length > 1 ? { ...item, pairedItem: { item: index } } : item);
            }
        }
        catch (error)
        {
            context.saveFailedExecution(new NodeOperationError(context.getNode(), error instanceof Error ? error : new Error('Message conversion failed')));
            release();
            return;
        }
        if (signal.aborted || !canEmit()) { release(); return; }
        try
        {
            context.emit([items], undefined, completion);
        }
        catch
        {
            release();
            context.saveFailedExecution(new NodeOperationError(context.getNode(), 'Triggered execution could not start'));
            return;
        }
        const result = await Promise.race([resultPromise, aborted]);
        const acknowledge = completedBoundary(result) || (options.onExecutionError === 'acknowledge' && result?.status === 'error');
        if (signal.aborted || !acknowledge)
        {
            release();
            return;
        }
        for (const [index, message] of messages.entries())
        {
            if (signal.aborted) { release(messages.slice(index)); return; }
            // Once confirmation starts, its timeout/disconnect is ambiguous: do not NAK it.
            try
            {
                const confirmed = await message.ackAck({ timeout: options.ackTimeoutMs });
                if (!confirmed) throw new Error('ACK not confirmed');
            }
            catch
            {
                context.saveFailedExecution(new NodeOperationError(context.getNode(), 'NATS ACK confirmation is unknown; the message may be redelivered'));
            }
            unsettled.delete(message);
        }
    }
    finally
    {
        clearInterval(timer);
        if (abortHandler) signal.removeEventListener('abort', abortHandler);
    }
}
