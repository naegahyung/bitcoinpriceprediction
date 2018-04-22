const axios = require('axios');
const fs = require('fs');

const dominanceUrl = 'https://graphs2.coinmarketcap.com/global/dominance';
const marketCapUrl = 'https://graphs2.coinmarketcap.com/global/marketcap-total/'
/**
 * 1367174820000 is 4/28/13, start of data
 * Will be fetching dominance % every 5 minute data points 
 * by requesting data per day. Need to check proxy host before running.
 * @param {Long} start unix timestamp of first date of data 
 */
const fetchDataAndSave = async (url, dataKey) => {
  const startTime = 1367174820000;
  const hour = 86400000;
  const current = 1522954020000; 
  let agg = [];
  for (let i = startTime; i < current; i = i + hour) {
    try {
      const res = await axios.get(`${url}/${i}/${i + hour}`, { 
        proxy: { host: '104.17.138.178', port: 443 } 
      });
      agg = agg.concat(res.data[dataKey]);
      console.log(`Currently on ${i}, agg size: ${agg.length}`);
    } catch (e) {
      console.log(`failed at ${i}`);
      console.error(e);
      //process.exit();
    }
  }
  fs.writeFile(`${dataKey}data.csv`, agg.join('\n'), (err) => {
    if (err) {
      return console.log(err);
    }
    console.log('File created!');
  })
}

//fetchDataAndSave(dominanceUrl, 'bitcoin')
//fetchDataAndSave(marketCapUrl, 'market_cap_by_available_supply');
