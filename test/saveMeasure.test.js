const BigQueryConnector = require('../index.js');

const test = new BigQueryConnector();

test
  .init(require('../config.js'), {
    errors: {
      PreconditionError: Error
    },
    log: console
  })
  .then(() => {
    test
      .saveMeasure({
        probeName: 'test',
        data: {
          'field_string': 'some other string',
          'field_int': 12533,
          'field_bool': true,
          'field_timestamp': Date.now()
        }
      })
      .then(result => {
        result.forEach(message => {
          console.log(message);
        });
      })
      .catch(error => {
        console.error(error);
        error.response.insertErrors.forEach(insertError => {
          console.error(insertError);
        });
      });
  })
  .catch(err => {
    console.error(`Something went wrong initializing: ${err.message}`);
  });
