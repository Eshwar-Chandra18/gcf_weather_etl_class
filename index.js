const {Storage} = require('@google-cloud/storage');
const csv = require('csv-parser');

exports.readObservation = (file, context) => {
     console.log(`  Event: ${context.eventId}`);
     console.log(`  Event Type: ${context.eventType}`);
     console.log(`  Bucket: ${file.bucket}`);
     console.log(`  File: ${file.name}`);

    const gcs = new Storage();

    const dataFile = gcs.bucket(file.bucket).file(file.name);

    dataFile.createReadStream()
    .on('error', () => {
        // Handle an error
        console.error(error);
    })
    .pipe(csv())
    .on('data', (row) => {
        // Log row data
        // console.log(row);
        printDict(row);
    })
    .on('end', () => {
        // Handle end of CSV
        console.log('End!');
    })
}

// HELPER FUNCTIONS

function printDict(row) {
    for (let key in row) {
        console.log(key + ' : ' + row[key]);
        console.log(`${key} : ${row[key]}`);
    }
}

function convertNegative9999ToNull(value) {
    // Convert -9999 to null
    return (value === -9999) ? null : value;
  }
  
  function convertToDecimal(value) {
    // Convert to decimal by dividing by 10
    return (value !== null) ? value / 10 : null;
  }
  
  function transformNumericFields(obj) {
    const numericFields = ['year', 'month', 'day', 'hour', 'winddirection', 'sky', 'airtemp', 'dewpoint', 'pressure', 'windspeed', 'precip1hour', 'precip6hour'];
  
    for (let key of numericFields) {
      if (obj.hasOwnProperty(key)) {
        if (key === 'station') {
          // Special case for 'station' field
          obj[key] = (obj[key] === '724380-93819') ? obj[key] : null;
        } else {
          // Common case for other numeric fields
          obj[key] = convertNegative9999ToNull(obj[key]);
          obj[key] = convertToDecimal(obj[key]);
        }
      }
    }
  
    return obj;
  }