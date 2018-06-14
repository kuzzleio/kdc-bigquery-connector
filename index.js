/*
 * Kuzzle, a backend software, self-hostable and ready to use
 * to power modern apps
 *
 * Copyright 2015-2018 Kuzzle
 * mailto: support AT kuzzle.io
 * website: http://kuzzle.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


const
  BigQuery = require('@google-cloud/bigquery'),
  debug = require('debug')('kuzzle:kdc:bigQuery'),
  Bluebird = require('bluebird'),
  Promise = Bluebird;

/**
 * @typedef {{name: string, type: string, mode: string}} BigQueryField
 * @typedef {{fields: BigQueryField[]}} BigQuerySchema
 * @typedef {{tableName: string}|object} ProbeConfiguration
 */

/**
 * Allows to forward measures coming from [KDC Probes](https://github.com/kuzzleio/kuzzle-enterprise-probe)
 * to Google BigQuery.
 *
 * @class BigQueryConnector
 * @property {KuzzlePluginContext} context
 * @property {object<string,ProbeConfiguration>} probes
 */
class BigQueryConnector {
  constructor() {
    this.hooks = {};
    this.probes = {};
    this.dataSet = null;
  }

  /**
   * @param {object} customConfig
   * @param {KuzzlePluginContext} context
   */
  init(customConfig, context) {
    this.context = context;

    const probePluginName = customConfig.probePluginName || 'kuzzle-enterprise-probe';
    this.hooks[`plugin-${probePluginName}:receivedMeasure`] = 'saveMeasure';

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
   * @param {object} probe The probe object (specified in the configuration).
   * @param {string} probeName The probe Name
   * @return {Promise}
   */
  createTableIfNotExists(probe, probeName) {
    const tableName = getTableForProbe(this.probes, probeName);

    if (!tableName) {
      return Promise.resolve();
    }

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
        const schema = getSchemaForProbe(probe);
        return this.bigQuery
          .dataset(this.dataSet)
          .createTable(
          tableName,
          { schema }
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
   * @param {object} measure
   * @return {void}
   */
  saveMeasure(measure) {
    // check that the probe that generated the measure is being watched
    // by this plugin.
    const tableName = getTableForProbe(this.probes, measure.probeName);

    if (!tableName) {
      return;
    }

    // extract the data from the measure and insert it in the table
    // (add the timestamp if needed)
    // whose name corresponds with the name of the probe.
    const data = extractMeasureData(measure.data);
    this.context.log.info(`-----------------------------------------`);
    this.context.log.info(data);
    this.context.log.info(`-----------------------------------------`);
    if (
      this.probes[measure.probeName] &&
      typeof this.probes[measure.probeName].timestamp != 'undefined' &&
      this.probes[measure.probeName].timestamp
    ) {
      data[0].timestamp = Math.round((new Date()).getTime() / 1000);
    }

    debug(`Received measure from probe ${measure.probeName}`);
    debug(JSON.stringify(data));

    this.bigQuery
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
 * @param {object<string, ProbeConfiguration>} probes
 * @param {string} probeName
 * @return {string|null} The name of the table corresponding to the probe. Null if the probe is not being tracked.
 */
function getTableForProbe(probes, probeName) {
  if (!probes[probeName]) {
    return null;
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
 * @param {object} probe The probe object (specified in the configuration).
 * @return {BigQuerySchema} The generated schema.
 */
function getSchemaForProbe(probe) {
  if (probe.schema) {
    if (probe.type === "watcher" && probe.timestamp) {
      probe.schema.fields.push(
        {
          "name": "timestamp",
          "type": "TIMESTAMP",
          "mode": "REQUIRED"
        }
      );
    }
    return probe.schema;
  }

  if (!probe.type) {
    throw new Error('Type field is mandatory in probes that do not provide schema');
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
 * @param {string[]} hooks The list of events counted by the monitor.
 * @return {BigQuerySchema} The schema.
 */
function buildMonitorSchema(hooks) {
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

  return {
    fields: schema
  };
}

/**
 * Extracts the data from the measure (watchers and samplers pack the data in
 * a `content` attribute) and normalizes the attribute names to make them
 * BigQuery table-compliant.
 *
 * @param {object} data
 * @return {object}
 */
function extractMeasureData(data) {
  if (data.content) {
    let extractedData = [flattenObject(data.content)];

    return extractedData.map(datum => {
      return normalizeMeasureData(datum);
    });
  }

  return normalizeMeasureData(data);
}

/**
 * Normalizes all the attributes names of a measure's data.
 *
 * @param {object} data The measure data.
 * @return {object} The normalized version.
 */
function normalizeMeasureData(data) {
  let normalizedData = {};

  Object.keys(data).forEach(key => {
    normalizedData[normalizeFieldName(key)] = data[key];
  });

  return normalizedData;
}

/**
 * Normalizes a field name to be BigQuery-compliant.
 *
 * @param {string} fieldName
 * @return {string}
 */
function normalizeFieldName(fieldName) {
  return fieldName.replace(/[^A-Z^a-z^0-9^_]/g, '_');
}

/**
 * Flatten nested object
 *
 * @param {Object} object
 * @return {Array}
 */
function flattenObject(object) {
  var toReturn = {};
  for (var i in object) {
    if (!object.hasOwnProperty(i)) continue;
    if ((typeof object[i]) == 'object' && object[i] !== null) {
      var flatObject = flattenObject(object[i]);
      for (var x in flatObject) {
        if (!flatObject.hasOwnProperty(x)) continue;
        toReturn[x] = flatObject[x];
      }
    } else {
      toReturn[i] = object[i];
    }
  }
  return toReturn;
};

module.exports = BigQueryConnector;
