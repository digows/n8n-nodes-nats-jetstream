import type { ICredentialType, INodeProperties, Icon } from 'n8n-workflow';

export class JetstreamNatsApi implements ICredentialType
{
    name = 'jetstreamNatsApi';
    displayName = 'JetStream NATS API';
    icon: Icon = { light: 'file:../nodes/JetstreamNats/jetstream.svg', dark: 'file:../nodes/JetstreamNats/jetstream.svg' };
    documentationUrl = 'https://github.com/digows/n8n-nodes-jetstream-nats#credentials';
    properties: INodeProperties[] = [
        { displayName: 'Server Addresses', name: 'servers', type: 'string', default: 'nats://localhost:4222', required: true, description: 'Comma- or whitespace-separated NATS server addresses without embedded credentials' },
        { displayName: 'Connection Name', name: 'connectionName', type: 'string', default: '' },
        { displayName: 'Connection Timeout (ms)', name: 'connectionTimeoutMs', type: 'number', default: 10000, typeOptions: { minValue: 1 } },
        { displayName: 'JetStream Domain', name: 'domain', type: 'string', default: '', description: 'Optional JetStream domain; leave empty for the default domain' },
        { displayName: 'Authentication', name: 'authentication', type: 'options', default: 'none', options: [
            { name: 'None', value: 'none' }, { name: 'Username/Password', value: 'userPassword' }, { name: 'Token', value: 'token' }, { name: 'NKey', value: 'nkey' }, { name: 'Credentials Content', value: 'creds' },
        ] },
        { displayName: 'Username', name: 'username', type: 'string', default: '', required: true, displayOptions: { show: { authentication: ['userPassword'] } } },
        { displayName: 'Password', name: 'password', type: 'string', default: '', required: true, typeOptions: { password: true }, displayOptions: { show: { authentication: ['userPassword'] } } },
        { displayName: 'Token', name: 'token', type: 'string', default: '', required: true, typeOptions: { password: true }, displayOptions: { show: { authentication: ['token'] } } },
        { displayName: 'NKey Private Seed', name: 'seed', type: 'string', default: '', required: true, typeOptions: { password: true }, displayOptions: { show: { authentication: ['nkey'] } } },
        { displayName: 'Credentials Content', name: 'creds', type: 'string', default: '', required: true, typeOptions: { password: true }, description: 'Complete NATS .creds file content, not a filesystem path', displayOptions: { show: { authentication: ['creds'] } } },
        { displayName: 'TLS', name: 'tls', type: 'boolean', default: false, description: 'Whether to require TLS with server certificate verification' },
        { displayName: 'CA Certificate (PEM)', name: 'ca', type: 'string', default: '', displayOptions: { show: { tls: [true] } } },
        { displayName: 'Client Certificate (PEM)', name: 'clientCertificate', type: 'string', default: '', typeOptions: { password: true }, displayOptions: { show: { tls: [true] } } },
        { displayName: 'Client Private Key (PEM)', name: 'clientKey', type: 'string', default: '', typeOptions: { password: true }, displayOptions: { show: { tls: [true] } } },
    ];
}
