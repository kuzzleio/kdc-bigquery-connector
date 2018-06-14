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
  should = require('should'),
  sinon = require('sinon'),
  rewire = require('rewire'),
  BigQueryConnector = rewire('../index.js'),
  Promise = require('bluebird');

describe('BigQueryConnector', () => {
  const
    context = {
        errors: {
          PreconditionError: Error
        },
        log: console
      },
    config = {
      projectId: 'toto',
      credentials: {
        echo: 'LOL'
      },
      dataSet: 'LULZ',
      probes: ['probe_1', 'probe_2']
    };
  let
    bigQueryConnector,
    existsStub = sinon.stub(),
    createTableStub = sinon.stub(),
    insertStub = sinon.stub(),
    BigQueryMock = function bqMock () {
      return {
        dataset() {
          return {
            table () {
              return {
                exists: existsStub,
                insert: insertStub
              };
            },
            createTable: createTableStub
          };
        }
      };
    };

  beforeEach(() => {
    BigQueryConnector.__set__({
      BigQuery: BigQueryMock
    });
    bigQueryConnector = new BigQueryConnector();
  });

  describe('#init', () => {
    it('should throw if projectId is not provided in config', () => {
      should(() => bigQueryConnector.init({}, context)).throw(/The projectId configuration is mandatory/i);
    });

    it('should throw if credentials is not provided in config', () => {
      should(() => bigQueryConnector.init({
        projectId: 'toto'
      }, context)).throw(/The credentials configuration is mandatory/i);
    });

    it('should throw if dataSet is not provided in config', () => {
      should(() => bigQueryConnector.init({
        projectId: 'toto',
        credentials: {
          echo: 'LOL'
        }
      }, context)).throw(/The dataSet configuration is mandatory/i);
    });

    it('should throw if probes is not provided in config', () => {
      should(() => bigQueryConnector.init({
        projectId: 'toto',
        credentials: {
          echo: 'LOL'
        },
        dataSet: 'LULZ'
      }, context)).throw(/The probes configuration is mandatory/i);
    });

    it('should call createTableIfNotExists for every probe', (done) => {
      sinon.stub(bigQueryConnector, 'createTableIfNotExists').returns(Promise.resolve());
      bigQueryConnector
        .init(config, context)
        .then(() => {
          should(bigQueryConnector.createTableIfNotExists.calledWith('probe_1')).eql(true);
          should(bigQueryConnector.createTableIfNotExists.calledWith('probe_2')).eql(true);
        })
        .finally(() => {
          bigQueryConnector.createTableIfNotExists.restore();
          done();
        });
    });
  });

  describe('#createTableIfNotExists', () => {
    it('should resolve if the probe is not listened or the table name is invalid ', () => {
      should(bigQueryConnector.createTableIfNotExists({}, 'probe_1')).be.fulfilled();
    });

    it('should do nothing if the table exists', () => {
      return BigQueryConnector.__with__({
        getTableForProbe () { return 'probe_table'; },
      })(() => {
        existsStub = sinon.stub().usingPromise(Promise).resolves([true]);
        bigQueryConnector.bigQuery = new BigQueryMock();

        return bigQueryConnector
          .createTableIfNotExists({}, 'probe_1')
          .then(() => {
            return should(createTableStub.called).eql(false);
          })
          .catch(error => {
            throw new Error('This thing failed', error);
          })
          .finally(() => {
            existsStub = sinon.stub();
          });
      });
    });

    it('should call createTable if the table does not exist', () => {
      return BigQueryConnector.__with__({
        getSchemaForProbe: () => { return []; },
        getTableForProbe: () => { return 'some_table'; }
      })(() => {
        existsStub = sinon.stub().usingPromise(Promise).resolves([false]);
        createTableStub = sinon.stub().usingPromise(Promise).resolves(true);
        bigQueryConnector.bigQuery = new BigQueryMock();

        return bigQueryConnector
          .createTableIfNotExists({}, 'probe_1')
          .then(() => {
            return should(createTableStub.called).eql(true);
          })
          .catch(error => {
            throw new Error('This thing failed', error);
          })
          .finally(() => {
            existsStub = sinon.stub();
            createTableStub = sinon.stub();
          });
      });
    });

    it('should reject if createTable failed', () => {
      return BigQueryConnector.__with__({
        getSchemaForProbe: () => { return []; },
        getTableForProbe: () => { return 'some_table'; }
      })(() => {
        existsStub = sinon.stub().usingPromise(Promise).resolves([false]);
        createTableStub = sinon.stub().usingPromise(Promise).rejects(false);
        bigQueryConnector.bigQuery = new BigQueryMock();

        return should(bigQueryConnector
          .createTableIfNotExists({}, 'probe_1'))
          .be.rejected()
          .then(() => {
            existsStub = sinon.stub();
            createTableStub = sinon.stub();
          });
      });
    });
  });

  describe('#saveMeasure', () => {
    it('should do nothing if the probe is not watched', () => {
      const extractMeasureDataStub = sinon.stub();
      return BigQueryConnector.__with__({
        extractMeasureData: extractMeasureDataStub,
        getTableForProbe: () => { return null; }
      })(() => {
        bigQueryConnector.saveMeasure({probeName: 'some_probe'});
        should(extractMeasureDataStub.called).eql(false);
      });
    });

    it('should save the measure if the probe is watched', () => {
      const extractMeasureDataStub = sinon.stub().returns({some: 'data'});
      return BigQueryConnector.__with__({
        extractMeasureData: extractMeasureDataStub,
        getTableForProbe: () => { return 'some_probe'; }
      })(() => {
        insertStub.resolves();
        bigQueryConnector.bigQuery = new BigQueryMock();
        bigQueryConnector.context = context;
        bigQueryConnector.saveMeasure({probeName: 'some_probe'});
        should(extractMeasureDataStub.called).eql(true);
        should(insertStub.called).eql(true);
        insertStub = sinon.stub();
      });
    });
  });

  describe('#getTableForProbe', () => {
    const getTableForProbe = BigQueryConnector.__get__('getTableForProbe');

    it('should return null if the probe is not listened', () => {
      should(getTableForProbe({}, 'some_probe')).eql(null);
    });

    it('should return the probe name if the probe has no table name', () => {
      should(getTableForProbe({
        some_probe: {}
      }, 'some_probe')).eql('some_probe');
    });

    it('should return the table name if the probe has one', () => {
      should(getTableForProbe({
        some_probe: {
          tableName: 'some_table'
        }
      }, 'some_probe')).eql('some_table');
    });
  });

  describe('#getSchemaForProbe', () => {
    const getSchemaForProbe = BigQueryConnector.__get__('getSchemaForProbe');

    it('should return the schema if the probe provides one', () => {
      should(getSchemaForProbe({
        schema: []
      })).eql([]);
    });

    it('should throw if the probe has no type nor schema', () => {
      should(() => { getSchemaForProbe({}); })
        .throw(/Type field is mandatory in probes that do not provide schema/);
    });

    it('should throw it the probe is a monitor with no hooks', () => {
      should(() => { getSchemaForProbe({
        type: 'monitor'
      }); }).throw(/Monitor probes must have an "hooks" field, of type Array./);

      should(() => { getSchemaForProbe({
        type: 'monitor',
        hooks: 'blabla'
      }); }).throw(/Monitor probes must have an "hooks" field, of type Array./);
    });

    it('should call buildMonitorSchema if the monitor probe is well-formed', () => {
      const buildMonitorSchemaStub = sinon.stub();

      BigQueryConnector.__with__({
        buildMonitorSchema: buildMonitorSchemaStub
      })(() => {
        getSchemaForProbe({
          type: 'monitor',
          hooks: ['one:hook']
        });
        should(buildMonitorSchemaStub.calledWith(['one:hook'])).eql(true);
      });
    });

    it('should return counter schema if probe is a counter', () => {
      should(getSchemaForProbe({type: 'counter'})).eql({
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
      });
    });

    it('should throw if probe is of any other type and provides no schema', () => {
      should(() => { getSchemaForProbe({type: 'watcher'}); }).throw();
    });
  });

  describe('#buildMonitorSchema', () => {
    const buildMonitorSchema = BigQueryConnector.__get__('buildMonitorSchema');

    it('should call normalizeFieldName for every hook', () => {
      const normalizeFieldNameStub = sinon.stub().returns('my_nigga');
      BigQueryConnector.__with__({
        normalizeFieldName: normalizeFieldNameStub
      })(() => {
        buildMonitorSchema(['my:nigga', 'nigga:nigga']);
        should(normalizeFieldNameStub.calledTwice).eql(true);
      });
    });

    it('should build a well-formed monitor schema', () => {
      const normalizeFieldNameStub = sinon.stub().returns('my_nigga');
      BigQueryConnector.__with__({
        normalizeFieldName: normalizeFieldNameStub
      })(() => {
        should(buildMonitorSchema(['my:nigga'])).eql({
          fields: [
            {
              name: 'my_nigga',
              type: 'INTEGER',
              mode: 'NULLABLE'
            },
            {
              name: 'timestamp',
              type: 'TIMESTAMP',
              mode: 'REQUIRED'
            }
          ]
        });
      });
    });
  });

  describe('#extractMeasureData', () => {
    const extractMeasureData = BigQueryConnector.__get__('extractMeasureData');

    it('should call normalizeMeasureData for every item in the content array', () => {
      const normalizeMeasureDataStub = sinon.stub();

      BigQueryConnector.__with__({
        normalizeMeasureData: normalizeMeasureDataStub,
      })(() => {
        extractMeasureData({
          content: [ 'firstMeasure', 'secondMeasure' ]
        });
        should(normalizeMeasureDataStub.calledWith('firstMeasure')).eql(true);
        should(normalizeMeasureDataStub.calledWith('secondMeasure')).eql(true);
      });
    });

    it('should call normalizeMeasureData for the measure', () => {
      const normalizeMeasureDataStub = sinon.stub();

      BigQueryConnector.__with__({
        normalizeMeasureData: normalizeMeasureDataStub
      })(() => {
        const theMeasure = {
          firstThing: 'myNigga',
          secondThing: 'niggaNigga'
        };
        extractMeasureData(theMeasure);
        should(normalizeMeasureDataStub.calledWith(theMeasure)).eql(true);
      });
    });
  });

  describe('#normalizeMeasureData', () => {
    const normalizeMeasureData = BigQueryConnector.__get__('normalizeMeasureData');

    it('should normalize every field of the measure', () => {
      const normalizeFieldNameStub = sinon.stub();

      BigQueryConnector.__with__({
        normalizeFieldName: normalizeFieldNameStub
      })(() => {
        normalizeMeasureData({
          firstField: 'myNigga',
          secondField: 'niggaNigga'
        });
        should(normalizeFieldNameStub.calledWith('firstField')).eql(true);
        should(normalizeFieldNameStub.calledWith('secondField')).eql(true);
      });
    });

    const flattenObject = BigQueryConnector.__get__('flattenObject');

    it('should flatten nested object', () => {
      const obj = {
        'parent': {
          'child1': 'value1',
          'child2': 'value2'
        }
      };
      should(flattenObject(obj)).eql(
        {
          'child1': 'value1',
          'child2': 'value2'
        }
      );
    });

  });

  describe('#normalizeFieldName', () => {
    const normalizeFieldName = BigQueryConnector.__get__('normalizeFieldName');

    it('should convert non-allowed characters to underscore', () => {
      should(normalizeFieldName('a:weird-string.to%be/converted')).eql('a_weird_string_to_be_converted');
    });
  });
});
