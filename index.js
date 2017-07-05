const
  BigQuery = require('@google-cloud/bigquery'),
  Bluebird = require('bluebird');

class BigQueryConnector {
  constructor () {
  }

  /**
   * @param  {object} customConfig
   * @param {KuzzlePluginContext} context
   */
  init (customConfig, context) {
    this.context = context;

    if (!customConfig.projectId) {
      throw new this.context.errors.PreconditionError('kdc-bigquery-connector: The projectId configuration is mandatory');
    }

    if (!customConfig.credentials) {
      throw new this.context.errors.PreconditionError('kdc-bigquery-connector: The credentials configuration is mandatory');
    }

    const options = {
      projectId: customConfig.projectId,
      credentials: customConfig.credentials,
      promise: Bluebird
    };

    this.bigQuery = new BigQuery(options);
  }
}

// module.exports = BigQueryConnector;

const test = new BigQueryConnector();

test.init(require('./config.js'), {
  errors: {
    PreconditionError: () => {}
  }
});

test.bigQuery.getDatasets({}).then(datasets => console.log(JSON.stringify(datasets, null, 2)));
