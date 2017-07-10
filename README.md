# KDC BigQuery Connector

> This plugin is currently Work In Progress.

## Pre-requisites

Create a `config.js` file at the root of the project with the following contents:

```javascript
module.exports = {
  projectId: '', // The BigQuery project ID
  dataSet: '', // The dataset within your project
  credentials: {
    type: 'service_account',
    project_id: '', // The BigQuery project ID
    private_key_id: '',
    private_key: '-----BEGIN PRIVATE KEY-----\n\n-----END PRIVATE KEY-----\n',
    client_email: 'big-query@kuzzle-probes.iam.gserviceaccount.com',
    client_id: '',
    auth_uri: 'https://accounts.google.com/o/oauth2/auth',
    token_uri: 'https://accounts.google.com/o/oauth2/token',
    auth_provider_x509_cert_url: 'https://www.googleapis.com/oauth2/v1/certs',
    client_x509_cert_url: ''
  },
  // The following probes serve as an example
  probes: {
    watcher_probe: {
      type: 'watcher',
      tableName: 'test_gen',
      schema: {
        fields: [
          {
            name: 'field_string',
            type: 'STRING',
            mode: 'NULLABLE'
          },
          {
            name: 'field_int',
            type: 'INTEGER',
            mode: 'NULLABLE'
          },
          {
            name: 'field_bool',
            type: 'BOOLEAN',
            mode: 'REQUIRED'
          },
          {
            name: 'field_timestamp',
            type: 'TIMESTAMP',
            mode: 'REQUIRED'
          }
        ]
      }
    },
    monitor_probe: {
      type: 'monitor',
      tableName: 'test_monitor',
      events: ['some:event', 'some:otherevent', 'andyet:anotherone']
    },
    counter_probe: {
      type: 'counter',
      tableName: 'test_counter',
    }
  }
};

```

## Run a test

You can run a test (make sure your config file is well-formed) by running

```bash
node tests/saveMeasure.test.js
```

