const
  BigQuery = require('@google-cloud/bigquery'),
  Bluebird = require('bluebird');

/**
 * @class BigQueryConnector
 */
class BigQueryConnector {
  constructor () {
    this.hooks = {};
    this.probes = {};
    this.dataSet = null;
  }

  /**
   * @param {Object} customConfig
   * @param {KuzzlePluginContext} context
   */
  init (customConfig, context) {
    this.context = context;

    const probePluginName = customConfig.probePluginName || 'kuzzle-enterprise-probe';
    this.hooks[`plugin-${probePluginName}:saveMeasure`] = 'saveMeasure';

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

    if (!customConfig.dataSet) {
      throw new this.context.errors.PreconditionError('kdc-bigquery-connector: The dataSet configuration is mandatory');
    }
    this.dataSet = customConfig.dataSet;

    if (!customConfig.probes) {
      throw new this.context.errors.PreconditionError('kdc-bigquery-connector: The probes configuration is mandatory');
    }
    this.probes = customConfig.probes;

    const promises = [];
    Object.keys(this.probes).forEach(probeName => {
      promises.push(this.createTableIfNotExists(this.probes[probeName], probeName));
    });

    return Promise.all(promises);
  }

  createTableIfNotExists (probe, probeName) {
    const tableName = getTableForProbe(this.probes, probeName);
    return this.bigQuery
      .dataset(this.dataSet)
      .table(tableName)
      .exists()
      .then(exists => {
        /* eslint eqeqeq: 0 */
        if (exists == 'false') {
          return Promise.reject();
        }
        console.log(`Table ${tableName} exists. Done.`);
      })
      .catch(() => {
        console.log(`Table ${tableName} does not exist. Creating.`);
        return this.bigQuery
          .dataset(this.dataSet)
          .createTable(
            tableName,
            { schema: probe.schema }
          );
      })
      .catch(err => {
        console.error(`Something went wrong while creating table for probe ${probeName}: ${err.message}`);
        return Promise.reject();
      });
  }

  /**
   * Saves the measure to BigQuery.
   *
   * @param  {Object} measure
   * @return {Promise}
   */
  saveMeasure (measure) {
    // check that the probe that generated the measure is being watched
    // by this plugin.
    const tableName = getTableForProbe(this.probes, measure.probeName);

    if (!tableName) {
      return Promise.reject();
    }

    // extract the data from the measure and insert it in the table
    // whose name corresponds with the name of the probe.
    return this.bigQuery
      .dataset(this.dataSet)
      .table(tableName)
      .insert(measure.data);
  }
}

/**
 * @param  {Object} probes
 * @param  {String} probeName
 * @return {String} The name of the table corresponding to the probe. Null if
 *                  the probe is not being tracked.
 */
function getTableForProbe (probes, probeName) {
  if (!probes[probeName]) {
    return;
  }

  if (!probes[probeName].tableName) {
    return probeName;
  }
  return probes[probeName].tableName;
}

module.exports = BigQueryConnector;
