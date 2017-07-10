# KDC BigQuery Connector

> This plugin is currently Work In Progress.

This plugin enables a Kuzzle Data Collector (see the [Enterprise Probe Plugin](https://github.com/kuzzleio/kuzzle-enterprise-probe)) to forward measures to Google BigQuery, for advanced analytics.

This is achieved by listening the `plugin-kuzzle-enterprise-probe:saveMeasure` custom event (triggered by the Enterprise Probe Plugin) and by analyzing its payload against the configuration. If measures correspond to probes specified in the configuration, they are formatted and sent to Google BigQuery via the GoogleCloud SDK.

The tables corresponding to the measures in BigQuery are automatically created if they don't exist.

## Pre-requisites

This plugin will not initialize unless a valid configuration is provided. Please refer to the [Kuzzle Plugins Reference](http://docs.kuzzle.io/plugins-reference/managing-plugins/#configuring-plugins) to learn how to configure a plugin.

## Configuration

Below is an example of configuration:

```json
{
  "plugins": {
    "kdc-bigquery-connector": {
      "projectId": "your-project-id",
      "dataSet": "your-data-set",
      "credentials": {
        "type": "service_account",
        "project_id": "your-project-id",
        "private_key_id": "put-yours-here",
        "private_key": "put-yours-here",
        "client_email": "put-yours-here",
        "client_id": "put-yours-here",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://accounts.google.com/o/oauth2/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": ""
      },
      "probes": {
        "probe_watcher_1": {
          "type": "watcher",
          "tableName": "table_watcher",
          "schema": {
            "fields": [
              {
                "name": "field_1",
                "type": "STRING",
                "mode": "NULLABLE"
              },
              {
                "name": "field_2",
                "type": "INTEGER",
                "mode": "NULLABLE"
              }
            ]
          }
        },
        "probe_monitor_1": {
          "type": "monitor",
          "tableName": "table_monitor",
          "hooks": ["controller:hook", "anotherController:anotherHook"]
        },
        "probe_counter_1": {
          "type": "counter",
          "tableName": "table_counter"
        }
      }
    }
  }
}
```

### projectId
The projectId of your BigQuery project. **Must exist before running the plugin**

### dataSet
The dataset in you BigQuery project. **Must exist before running the plugin**

### credentials
The credentials used to log in the BigQuery service. Please refer to the [Big Query User Manual](https://googlecloudplatform.github.io/google-cloud-node/#/docs/bigquery/0.9.2/guides/authentication).

### probes
The probes you are "listening to" or, in other words, whose measures must be sent to BigQuery. These probes must be properly configured in the [Enterprise Probe Plugin](https://github.com/kuzzleio/kuzzle-enterprise-probe) and in the [Enterprise Probe Listener Plugin](https://github.com/kuzzleio/kuzzle-enterprise-probe-listener). Each probe must specify the following fields:

* `type` (mandatory): can be `monitor`, `counter`, `watcher`, `sampler`.
* `table_name` (optional): specifies the name of the table that will contain the measure data. If this field is not provided, the table name will be derived from the name of the probe.
* `hooks` (mandatory - only for `monitor`): an array of hook names, which corresponds to the hooks monitored by the probe. Hook names are normalized to match valid column names (BigQuery only allows numbers, letters and underscores in column names).
* `schema` (optional for `monitor` and `counter`, mandatory for `watcher` and `sampler`): the schema to apply to the table if created. Please refer to the [BigQuery Tables Reference](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#resource) to learn more about the format.
