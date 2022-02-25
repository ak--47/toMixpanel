//deps
import { createWriteStream, readFile, writeFile, statSync, mkdirSync, existsSync, readdir } from 'fs';
import { pipeline } from 'stream';
import { promisify } from 'util'
import * as path from 'path';
import fetch from 'node-fetch';
import { default as zip } from 'adm-zip';
import { default as gun } from 'node-gzip';
import { execSync } from 'child_process'
import dayjs from 'dayjs';
const streamOpts = { highWaterMark: Math.pow(2, 27) };



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

    let auth = "Basic " + Buffer.from(creds.apiKey + ":" + creds.apiSecret).toString('base64');
    let startForConsole = dayjs(options.start).format('MM-DD-YYYY THH');
    let endForConsole = dayjs(options.end).format('MM-DD-YYYY THH');
    console.log(`   calling /export amplitude api for ${startForConsole} - ${endForConsole}`)
    const response = await fetch(`${baseURL}?start=${options.start}&end=${options.end}`, {
        headers: {
            "Authorization": auth
        }

    });

    if (!response.ok) {
        console.log(`AMP API ERROR: ${response.status} ${response.statusText}`)
        return false;
    } 

    //download archive
    console.log('   downloading data...');
    writePath = path.resolve(`${dataPath}/downloaded`)
    try {
    await streamPipeline(response.body, createWriteStream(`${writePath}/data.zip`, streamOpts));
    }
    catch (e) {
        console.log(`error downloading data`)
        console.log(e)
    }
    const stats = statSync(`${writePath}/data.zip`);
    const fileSizeInBytes = stats.size;
    const fileSizeInMegabytes = fileSizeInBytes / 1000000.0;

    //unzip
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
        
        try {
            let source = escapeForShell(path.resolve(`${dataPath}/unzip/${file}`))
            let dest = escapeForShell(path.resolve(`${writePath}/${file.split('.gz')[0]}`))
            execSync(`gunzip -c ${source} > ${dest}`);
            let numLines = execSync(`wc -l ${dest}`);
            numEvents += Number(numLines.toString().split('/').map(x => x.trim())[0]);           
            
        } catch (e) {
            //counting events recieved
            console.log(`       gunzip FAIL! falling back on gun`)
            let dataFile = await readFilePromisified(path.resolve(`${dataPath}/unzip/${file}`));
            let gunzipped = await gun.ungzip(dataFile)
            let amplitudeRawData = gunzipped.toString('utf-8');
            let numOfLines = amplitudeRawData.split('\n').length - 1
            numEvents += numOfLines;
            await writeFilePromisified(`${writePath}/${file.split('.gz')[0]}`, amplitudeRawData);
        }       
       

    }

    if (numEvents > 0) {
        console.log(`   got ${smartCommas(numEvents)} events from amplitude...`);
    }
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