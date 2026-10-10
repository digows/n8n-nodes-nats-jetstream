import { configWithoutCloudSupport } from '@n8n/node-cli/eslint';

// Native TCP/TLS connections are self-hosted, not n8n Cloud verified.
export default [
    ...configWithoutCloudSupport,
    {
        files: ['lib/**/*.ts'],
        // Helpers are context-free; their errors are wrapped at the node boundary.
        rules: { '@n8n/community-nodes/require-node-api-error': 'off' },
    },
    {
        files: ['package.json'],
        // The approved self-hosted contract uses official SDK runtime dependencies.
        rules: { '@n8n/community-nodes/no-runtime-dependencies': 'off' },
    },
    {
        files: ['credentials/**/*.ts'],
        // NKey seeds, .creds and PEM private keys are secrets despite generic field names.
        rules: { '@n8n/community-nodes/credential-unnecessary-password': 'off' },
    },
];
