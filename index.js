/**
* npm install maxmind
* npm install csv
* npm install yargs
*/
let maxmind = require('maxmind');
let fs = require('fs');
let parse = require('csv-parse');
let stringify = require('csv-stringify');
let transform = require('stream-transform');
let argv = require('yargs').argv;
let koreanCountryName = require('./iso_3166-1_alpha-2_korean');
let koreanRegionName = require('./iso_3166-2_korean');

let geoIP2CityDB = argv.cityDB || 'GeoLite2-City.mmdb';
let geoIP2AsnDB = argv.asnDB || 'GeoLite2-ASN.mmdb';
let inFile = argv.in || null; //'escrow_20180627.csv';
let outFile = argv.out || null; //'escrow_20180627_geoIpData.csv';
let clientIpColName = argv.ipColName || 'client_ip';
let delimiter = argv.delimiter || ',';
let setKoreaName = true;

maxmind.open(geoIP2CityDB, (err, cityLookup) => {
maxmind.open(geoIP2AsnDB, (err, asnLookup) => {

  let parser = parse({delimiter: delimiter});
  let stringifier = stringify({delimiter: delimiter});
  let input = inFile ? fs.createReadStream(inFile) : process.stdin;
  let output = outFile? fs.createWriteStream(outFile) : process.stdout;

  let isHeader = true;
  let clientIpColumnIndex = 0;
  let transformer = transform((record, callback) => {
    // Null -> undefined (문자열로 'NULL' 인 경우 undefined 으로 바꿔줌)
    for (let i = 0;i < record.length; ++i){
      if (record[i] === 'NULL') record[i] = undefined;
    }

    // 헤더인 경우 client_ip를 찾아 낸다.
    if (isHeader) clientIpColumnIndex = record.indexOf(clientIpColName);

    // ip 값 가져옴
    let ip = record[clientIpColumnIndex];
    let geoIpData = getGeoIpData(ip, cityLookup, asnLookup);
    if (setKoreaName) geoIpData = changeNameForKorean(geoIpData);
    if (isHeader) { // header
      for (let key in geoIpData) record.push(key); // 헤더는 기존 record에 헤더만 추가한다
      isHeader = false; // 해더는 한번만 처리하면 끝
    } else { // data
      for (let key in geoIpData) record.push(geoIpData[key]); // 데이터는 기존 record에 데이터를 추가한다
    }
    callback(null, record);
  }, {parallel: 10});

  input.pipe(parser).pipe(transformer).pipe(stringifier).pipe(output);
});});


function getGeoIpData(ip, cityLookup, asnLookup) {
  let city = cityLookup.get(ip);
  let asn = asnLookup.get(ip);
  return {
    countryCode: getPropertySafe(city, ['country', 'iso_code']),
    countryName: getPropertySafe(city, ['country', 'names', 'en']),
    geonameId: getPropertySafe(city, ['subdivisions', 0, 'geoname_id']),
    regionCode: getPropertySafe(city, ['subdivisions', 0, 'iso_code']),
    regionName: getPropertySafe(city, ['subdivisions', 0, 'names', 'en']),
    city: getPropertySafe(city, ['city', 'names', 'en']),
    continent: getPropertySafe(city, ['continent', 'names', 'en']),
    accuracyRadius: getPropertySafe(city, ['location', 'accuracy_radius']),
    latitude: getPropertySafe(city, ['location', 'latitude']),
    longitude: getPropertySafe(city, ['location', 'longitude']),
    timeZone: getPropertySafe(city, ['location', 'time_zone']),
    postalCode: getPropertySafe(city, ['postal', 'code']),
    autonomousSystemNumber: getPropertySafe(asn, ['autonomous_system_number']),
    autonomousSystemOrganization: getPropertySafe(asn, ['autonomous_system_organization'])
  };
}

function changeNameForKorean(geoIpData) {
  if (!geoIpData) return geoIpData;
  if (koreanCountryName[geoIpData.countryCode])
    geoIpData.countryName = koreanCountryName[geoIpData.countryCode];
  let isoRegionCode = geoIpData.countryCode + '-' + geoIpData.regionCode;
  if (koreanRegionName[isoRegionCode])
    geoIpData.regionName = koreanRegionName[isoRegionCode];
  return geoIpData;
}

function getPropertySafe(obj, pathArray) {
  if (obj === undefined || obj === null || !pathArray || !pathArray.length) return obj;

  let cur = obj;
  for (let i = 0;i < pathArray.length; ++i) {
    cur = cur[pathArray[i]];
    if (cur === undefined || cur === null) return cur;
  }

  return cur;
}
