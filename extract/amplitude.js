//deps
import { createWriteStream, readFile, writeFile, statSync, mkdirSync, existsSync, readdir } from 'fs';
import { pipeline } from 'stream';
import { promisify } from 'util'
import * as path from 'path';
import fetch from 'node-fetch';
import { default as zip } from 'adm-zip';
import { default as gun } from 'node-gzip';
import { execSync } from 'child_process'




//docs: https://developers.amplitude.com/docs/export-api#export-api-parameters
//todo EU residency? https://developers.amplitude.com/docs/http-api-v2
const baseURL_US = `https://amplitude.com/api/2/export`
const baseURL_EU = `https://analytics.eu.amplitude.com/api/2/export`

const streamPipeline = promisify(pipeline);
const readFilePromisified = promisify(readFile);
const writeFilePromisified = promisify(writeFile);
const readDirPromisified = promisify(readdir);

async function main(creds, options, directory = "foo", isEU) {
    let baseURL = isEU ? baseURL_EU : baseURL_US;
    //usually just for testing
    if (!existsSync(`./savedData/${directory}`)) {
        mkdirSync(`./savedData/${directory}`);
    }

    let numEvents = 0;
    let dataPath = `./savedData/${directory}/`
    let writePath = `./savedData/${directory}/`


    //make some sub directories
    mkdirSync(`${dataPath}/downloaded`)
    mkdirSync(`${dataPath}/unzip`)
    mkdirSync(`${dataPath}/json`)

    let auth = "Basic " + Buffer.from(creds.apiKey + ":" + creds.apiSecret).toString('base64')
    console.log('   calling /export amplitude api...')
    const response = await fetch(`${baseURL}?start=${options.start}&end=${options.end}`, {
        headers: {
            "Authorization": auth
        }

    });

    if (!response.ok) throw new Error(`unexpected response ${response.statusText}`);

    //download archive
    console.log('   downloading data...');
    writePath = path.resolve(`${dataPath}/downloaded`)
    await streamPipeline(response.body, createWriteStream(`${writePath}/data.zip`));
    const stats = statSync(`${writePath}/data.zip`);
    const fileSizeInBytes = stats.size;
    const fileSizeInMegabytes = fileSizeInBytes / 1000000.0;

    //un zip
    console.log(`   unzipping data... (${fileSizeInMegabytes} MB)`)
    writePath = path.resolve(`${dataPath}/unzip`);
    let filesToUngzip = [];

    try {        
        execSync(`unzip -j ${escapeForShell(path.resolve(dataPath+"/downloaded/data.zip"))} -d ${escapeForShell(writePath)}`);
        let allFiles = await readDirPromisified(writePath);
        for (let file of allFiles) {
            if (file.includes(".json.gz")) {
                filesToUngzip.push(file)
            }
            
        }
        console.log(`       unzipped ${smartCommas(filesToUngzip.length)} files\n\n`)     
    } catch (e) {
        console.log(`unzip is not available... trying adm-zip`)
        const zipped = new zip(`${dataPath}/downloaded/data.zip`);
        var zipEntries = zipped.getEntries(); // an array of ZipEntry records


        zipEntries.forEach(function(zipEntry) {
            if (zipEntry.entryName.includes('json')) {
                filesToUngzip.push(zipEntry.entryName)
                zipped.extractEntryTo(zipEntry.entryName, `${writePath}`, false, true);


            }
        });
        filesToUngzip = filesToUngzip.map((name) => {
            return name.split('/')[1]
        })
    }





    //ungzip
    console.log(`   gunzipping data... (${smartCommas(filesToUngzip.length)} files)`);
    writePath = `${dataPath}/json`;
    for (let file of filesToUngzip) {

        let dataFile = await readFilePromisified(path.resolve(`${dataPath}/unzip/${file}`));
        let gunzipped = await gun.ungzip(dataFile)
        let amplitudeRawData = gunzipped.toString('utf-8');

        //counting events recieved        
        let numOfLines = amplitudeRawData.split('\n').length - 1
        numEvents += numOfLines;

        //await streamPipeline(gunzipped, createWriteStream(`${writePath}/${file.split('.gz')[0]}`));
        await writeFilePromisified(`${writePath}/${file.split('.gz')[0]}`, amplitudeRawData);

    }


    console.log(`   got ${smartCommas(numEvents)} events from amplitude...`);
    console.log('\n')
    let rawFileNames = await readDirPromisified(writePath)
    let exports = rawFileNames.map(filePath => path.resolve(`${writePath}/${filePath}`));
    //console.log(exports)
    return exports;
}

//utils
function smartCommas(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function escapeForShell(arg) {
    return `'${arg.replace(/'/g, `'\\''`)}'`;
}

// main(credentials, options, 'foo');

export default main;