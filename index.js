const
  BigQuery = require('@google-cloud/bigquery'),
  debug = require('debug')('kuzzle:kdc:bigQuery'),
  Bluebird = require('bluebird');

/**
 * Allows to forward measures coming from [KDC Probes](https://github.com/kuzzleio/kuzzle-enterprise-probe)
 * to Google BigQuery.
 *
 * @class BigQueryConnector
 */
class BigQueryConnector {
  constructor () {
    this.hooks = {
      'plugin-kuzzle-enterprise-probe:saveMeasure': 'saveMeasure'
    };
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

  /**
   * Creates a table for a given probe (and sets a schema) if it does not exist.
   *
   * @param  {Object} probe     The probe object (specified in the configuration).
   * @param  {String} probeName The probe Name
   * @return {Promise}
   */
  createTableIfNotExists (probe, probeName) {
    const tableName = getTableForProbe(this.probes, probeName);
    return this.bigQuery
      .dataset(this.dataSet)
      .table(tableName)
      .exists()
      .then(exists => {
        if (!exists[0]) {
          return Promise.reject();
        }
        console.info(`Table ${tableName} exists. Done.`);
      })
      .catch(() => {
        console.info(`Table ${tableName} does not exist. Creating.`);
        return this.bigQuery
          .dataset(this.dataSet)
          .createTable(
            tableName,
            { schema: getSchemaForProbe(probe) }
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
    const data = extractMeasureData(measure.data);
    debug(`Received measure from probe ${measure.probeName}`);
    debug(JSON.stringify(data));

    return this.bigQuery
      .dataset(this.dataSet)
      .table(tableName)
      .insert(data)
      .then(() => {
        this.context.log.info(`Saved measure from ${measure.probeName}`);
      })
      .catch(e => {
        this.context.log.error(`Something weird happened while saving the measure: ${e.message}`);
        debug(`Table: ${tableName}`);
        debug(data);
      });
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

/**
 * Infers a table schema for a given probe, based on its type (or its explicitly
 * specified schema).
 *
 * @param  {Object} probe     The probe object (specified in the configuration).
 * @return {Array}            The generated schema.
 */
function getSchemaForProbe (probe) {
  if (probe.schema) {
    return probe.schema;
  }

  if (probe.type === 'monitor') {
    if (!probe.hooks || !Array.isArray(probe.hooks)) {
      throw new Error('Monitor probes must have an "hooks" field, of type Array.');
    }
    return buildMonitorSchema(probe.hooks);
  }

  if (probe.type === 'counter') {
    return {
      fields: [
        {
          name: 'count',
          type: 'INTEGER',
          mode: 'REQUIRED'
        },
        {
          name: 'timestamp',
          type: 'TIMESTAMP',
          mode: 'REQUIRED'
        }
      ]
    };
  }

  throw new Error(`Schema is mandatory for probes of type ${probe.type}`);
}

/**
 * Builds a table schema based on the list of events a monitor probe listens to.
 *
 * @param  {Array} events The list of events counted by the monitor.
 * @return {Array}        The schema.
 */
function buildMonitorSchema (hooks) {
  const schema = hooks.map(hookName => {
    return {
      name: normalizeFieldName(hookName),
      type: 'INTEGER',
      mode: 'NULLABLE'
    };
  });
  schema.push({
    name: 'timestamp',
    type: 'TIMESTAMP',
    mode: 'REQUIRED'
  });

  return schema;
}

/**
 * Extracts the data from the measure (watchers and samplers pack the data in
 * a `content` attribute) and normalizes the attribute names to make them
 * BigQuery table-compliant.
 *
 * @param  {Object} data
 * @return {Object}
 */
function extractMeasureData (data) {
  if (data.content) {
    return data.content.map(datum => {
      return normalizeMeasureData(datum);
    });
  }

  return normalizeMeasureData(data);
}

/**
 * Normalizes all the attributes names of a measure's data.
 *
 * @param  {Object} data The measure data.
 * @return {Object}      The normalized version.
 */
function normalizeMeasureData (data) {
  let normalizedData = {};

  Object.keys(data).forEach(key => {
    normalizedData[normalizeFieldName(key)] = data[key];
  });

  return normalizedData;
}

/**
 * Normalizes a field name to be BigQuery-compliant.
 *
 * @param  {String} fieldName
 * @return {String}
 */
function normalizeFieldName (fieldName) {
  return fieldName.replace(/[^A-Z^a-z^0-9^_]/, '_');
}

module.exports = BigQueryConnector;
