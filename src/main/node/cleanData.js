const fs = require('fs');
const csv = require('csv-parser');
const moment = require('moment');

const recreateJSONfile = (file, fileName) => {
  const data = fs.readFileSync(file);
  const parsed = JSON.parse(data);
  const converted = parsed.HistoricalPoints.map(p => {
    let agg = [];
    let point = p.PointInTime;
    for (let i = point; i < point + 3600000 * 24; i = i + 3600000) {
      agg.push([ i / 1000, p.InterbankRate ]);
    }
    return agg;
  });
  const result = [].concat.apply([], converted);
  fs.writeFileSync(`./assets/${fileNAme}.csv`, result.join('\n'));
};

const cleanUpCSVfile = (fileName, stream) => {
  let count = 0;
  console.log("starting!");
  return new Promise((resolve, reject) => {
    fs.createReadStream(fileName)
    .pipe(csv())
    .on('data', function (data) {
      let timestamp = moment.utc(data.timestamp).valueOf() / 1000;
      let d = (`${timestamp},${data.price},${data.amount}`);
      stream.write(`${d}\n`);
      count++;
    })
    .on('end', function () {
      console.log(count);
    })
  })
};

const aggregateData = async () => {
  const stream = fs.createWriteStream('./assets/bitfinex/bitfinexUSD.csv', {'flags': 'a' });
  for(let i = 1; i < 18; i++) {
    await cleanUpCSVfile(`./assets/bitfinex/bitfinexRaw${i}.csv`, stream);
  }
};

const duplicateRows = () => {
  const stream = fs.createWriteStream('./assets/etc/JPYUSD1.csv', { 'flags': 'a' });
  fs.createReadStream('./assets/etc/JPYUSD.csv')
    .pipe(csv())
    .on('data', function (data) {
      for (let i = 0; i < 60; i++) {
        stream.write(`${parseInt(data.timestamp) + (60 * i)},${data.ratio}\n`)
      }
    })
    .on('end', function () {
      console.log('done');
    })
}

duplicateRows()