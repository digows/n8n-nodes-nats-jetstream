import { connect, credsAuthenticator, nkeyAuthenticator, type NatsConnection, type NodeConnectionOptions } from '@nats-io/transport-node';
import type { ICredentialDataDecryptedObject } from 'n8n-workflow';

export function connectionOptions(credentials: ICredentialDataDecryptedObject): NodeConnectionOptions
{
    const servers = requiredText(credentials.servers, 'Server addresses').split(/[\s,]+/).filter(Boolean);
    for (const server of servers)
    {
        const address = new URL(server.includes('://') ? server : `nats://${server}`);
        if (address.username || address.password)
        {
            throw new Error('Put authentication values in credential fields, not server addresses');
        }
        if (!['nats:', 'tls:'].includes(address.protocol))
        {
            throw new Error('Server addresses must use nats:// or tls://');
        }
    }
    const options: NodeConnectionOptions = {
        servers,
        name: optionalText(credentials.connectionName),
        timeout: positiveInteger(credentials.connectionTimeoutMs ?? 10000, 'Connection timeout'),
        maxReconnectAttempts: -1,
    };
    switch (credentials.authentication)
    {
        case 'none': break;
        case 'userPassword':
            options.user = requiredText(credentials.username, 'Username');
            options.pass = requiredSecret(credentials.password, 'Password');
            break;
        case 'token': options.token = requiredSecret(credentials.token, 'Token'); break;
        case 'nkey':
            options.authenticator = nkeyAuthenticator(new TextEncoder().encode(requiredText(credentials.seed, 'NKey seed')));
            break;
        case 'creds':
            // The SDK's credentials parser requires a newline after the final delimiter.
            options.authenticator = credsAuthenticator(new TextEncoder().encode(`${requiredSecret(credentials.creds, 'Credentials content')}\n`));
            break;
        default: throw new Error('Select a supported authentication mode');
    }
    const certificate = optionalText(credentials.clientCertificate);
    const privateKey = optionalText(credentials.clientKey);
    if (Boolean(certificate) !== Boolean(privateKey))
    {
        throw new Error('Client certificate and private key must be supplied together');
    }
    if (credentials.tls === true)
    {
        options.tls = { rejectUnauthorized: true, ca: optionalText(credentials.ca), cert: certificate, key: privateKey };
    }
    else if (certificate || privateKey || optionalText(credentials.ca))
    {
        throw new Error('Enable TLS to use PEM credentials');
    }
    return options;
}

export async function openConnection(credentials: ICredentialDataDecryptedObject): Promise<NatsConnection>
{
    const options = connectionOptions(credentials);
    try
    {
        return await connect(options);
    }
    catch
    {
        // Transport errors can contain authentication material or server addresses.
        throw new Error('NATS connection failed: check connectivity, authentication and TLS credentials');
    }
}

export function optionalText(value: unknown): string | undefined
{
    return typeof value === 'string' && value.trim() ? value.trim() : undefined;
}

export function requiredText(value: unknown, label: string): string
{
    const text = optionalText(value);
    if (!text) throw new Error(`${label} is required`);
    return text;
}

function requiredSecret(value: unknown, label: string): string
{
    if (typeof value !== 'string' || value.length === 0) throw new Error(`${label} is required`);
    return value;
}

export function positiveInteger(value: unknown, label: string): number
{
    if (typeof value !== 'number' || !Number.isSafeInteger(value) || value <= 0)
    {
        throw new Error(`${label} must be a positive safe integer`);
    }
    return value;
}

export async function withinTimeout<T>(operation: Promise<T>, milliseconds: number): Promise<T>
{
    let timer: ReturnType<typeof setTimeout> | undefined;
    try
    {
        return await Promise.race([operation, new Promise<never>((_, reject) =>
        {
            timer = setTimeout(() => reject(new Error('Operation timed out')), milliseconds);
        })]);
    }
    finally
    {
        clearTimeout(timer);
    }
}
