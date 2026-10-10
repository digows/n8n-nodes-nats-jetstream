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
}

export function completedBoundary(result: IRun | undefined): boolean
{
    if (!result?.data?.resultData || result.data.resultData.error) return false;
    if (result.status === 'success') return true;
    return result.status === 'waiting' && Boolean(result.waitTill) && Number.isFinite(new Date(result.waitTill!).getTime());
}

export async function deliverMessage(context: ITriggerFunctions, message: JsMsg, options: DeliveryOptions, signal: AbortSignal, connected: () => boolean): Promise<void>
{
    const completion = context.helpers.createDeferredPromise<IRun>();
    // Attach rejection handlers before decoding/emitting to avoid unhandled enqueue failures.
    const resultPromise = completion.promise.then(result => result, () => undefined);
    let abortHandler: (() => void) | undefined;
    const aborted = new Promise<undefined>(resolve =>
    {
        abortHandler = () => resolve(undefined);
        signal.addEventListener('abort', abortHandler, { once: true });
        if (signal.aborted) resolve(undefined);
    });
    const progress = () =>
    {
        if (connected()) message.working();
    };
    const timer = setInterval(() =>
    {
        try { progress(); }
        catch { context.logger.warn('NATS delivery progress could not be sent'); }
    }, options.progressIntervalMs);
    try
    {
        progress();
        let item;
        try
        {
            item = await messageItem(message, options.format, context.helpers);
        }
        catch (error)
        {
            context.saveFailedExecution(new NodeOperationError(context.getNode(), error instanceof Error ? error : new Error('Message conversion failed')));
            if (connected()) message.nak(options.nakDelayMs);
            return;
        }
        if (signal.aborted) return;
        try
        {
            context.emit([[item]], undefined, completion);
        }
        catch
        {
            if (connected()) message.nak(options.nakDelayMs);
            context.saveFailedExecution(new NodeOperationError(context.getNode(), 'Triggered execution could not start'));
            return;
        }
        const result = await Promise.race([resultPromise, aborted]);
        if (signal.aborted || !completedBoundary(result))
        {
            if (connected()) message.nak(options.nakDelayMs);
            return;
        }
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
    }
    finally
    {
        clearInterval(timer);
        if (abortHandler) signal.removeEventListener('abort', abortHandler);
    }
}
