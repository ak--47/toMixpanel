//forked from: https://github.com/ak--47/mpBatchImport-node

//DEPENDENCIES

import { readFile } from 'fs';
import { promisify } from 'util';
import {readFile as read} from 'fs/promises'
import fetch from 'node-fetch'; //https://www.npmjs.com/package/node-fetch
import md5 from 'md5'; //https://www.npmjs.com/package/md5
import isGzip from 'is-gzip'; //https://www.npmjs.com/package/is-gzip
import gun from 'node-gzip'; //https://www.npmjs.com/package/node-gzip

const readFilePromisified = promisify(readFile);


//CONFIG + LIMITS
const ENDPOINT_URL_US = `https://api.mixpanel.com/import`
const ENDPOINT_URL_EU = `https://api-eu.mixpanel.com/import`
const EVENTS_PER_BATCH = 2000
const BYTES_PER_BATCH = 2 * 1024 * 1024



async function main(credentials = {}, dataFile = ``, isEU, isAlreadyABatch = false) {
    let ENDPOINT_URL = isEU ? ENDPOINT_URL_EU : ENDPOINT_URL_US;
    let allData;

    if (isAlreadyABatch) {
        allData = dataFile
    } else {
        //LOAD data files
        let file = await read(dataFile, "utf-8").catch((e) => {
            console.error(`failed to load ${dataFile}... does it exist?\n`);
            console.log(`if you require some test data, try 'npm run generate' first...`);
            process.exit(1);
        });


        //UNIFY
        //if it's already JSON, just use that

        try {
            allData = JSON.parse(file)
        } catch (e) {
            //it's probably NDJSON, so iterate over each line
            try {
                allData = file.trim().split('\n').map(line => JSON.parse(line));
            } catch (e) {
                //if we don't have JSON or NDJSON... fail...
                console.log('failed to parse data... only valid JSON or NDJSON is supported by this script')
                console.log(e)
                return 0;
            }
        }

        console.log(`       parsed ${numberWithCommas(allData.length)} events from ${dataFile}`);

    }


    //CHUNK

    //max 2000 events per batch
    const batches = chunkForNumOfEvents(allData, EVENTS_PER_BATCH);

    //max 2MB size per batch
    const batchesSized = chunkForSize(batches, BYTES_PER_BATCH);


    //COMPRESS
    const compressed = await compressChunks(batchesSized)


    //FLUSH
    console.log(`       sending ${numberWithCommas(allData.length)} events in ${numberWithCommas(batches.length)} batches`);
    let numRecordsImported = 0;
    for (let eventBatch of compressed) {
        try {
        let result = await sendDataToMixpanel(credentials, eventBatch);
        // console.log(`   done ✅`)
        // console.log(`   mixpanel response:`)
        // console.log(result);
        //console.log('\n')        
        try {
        numRecordsImported += result.num_records_imported || 0;
        }
        catch (e) {
            
        }
        }
        catch (e) {

        }
    }

    //FINISH
    //console.log(`   successfully imported ${numberWithCommas(numRecordsImported)} events`)
    return numRecordsImported;

    //HELPERS
    function chunkForNumOfEvents(arrayOfEvents, chunkSize) {
        return arrayOfEvents.reduce((resultArray, item, index) => {
            const chunkIndex = Math.floor(index / chunkSize)

            if (!resultArray[chunkIndex]) {
                resultArray[chunkIndex] = [] // start a new chunk
            }

            resultArray[chunkIndex].push(item)

            return resultArray
        }, [])
    }

    function chunkForSize(arrayOfBatches, maxBytes) {
        return arrayOfBatches.reduce((resultArray, item, index) => {
            //assume each character is a byte
            const currentLengthInBytes = JSON.stringify(item).length

            if (currentLengthInBytes >= maxBytes) {
                //if the batch is too big; cut it in half
                //todo: make this is a little smarter
                let midPointIndex = Math.ceil(item.length / 2);
                let firstHalf = item.slice(0, midPointIndex);
                let secondHalf = item.slice(-midPointIndex);
                resultArray.push(firstHalf);
                resultArray.push(secondHalf);
            } else {
                resultArray.push(item)
            }

            return resultArray
        }, [])
    }

    async function compressChunks(arrayOfBatches) {
        const allBatches = arrayOfBatches.map(async function(batch) {
            return await gun.gzip(JSON.stringify(batch))
        });
        return Promise.all(allBatches);
    }

    async function sendDataToMixpanel(auth, batch) {
        let authString = 'Basic ' + Buffer.from(auth.username + ':' + auth.password, 'binary').toString('base64');
        let url = `${ENDPOINT_URL}?project_id=${auth.project_id}&strict=1`
        let options = {
            method: 'POST',
            headers: {
                'Authorization': authString,
                'Content-Type': 'application/json',
                'Content-Encoding': 'gzip'

            },
            body: batch
        }

        try {
            let req = await fetch(url, options);
            let res = await req.json();
            //console.log(`           ${JSON.stringify(res)}`)
            return res;

        } catch (e) {
            console.log(`   problem with request:\n${e}`)
        }
    }

    function numberWithCommas(x) {
        return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
    }

}



export default main;