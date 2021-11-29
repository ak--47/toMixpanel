//deps
import { readFile } from 'fs';
import { promisify } from 'util';
import fetch from 'node-fetch'; //https://www.npmjs.com/package/node-fetch


const readFilePromisified = promisify(readFile);

//CONFIG + LIMITS
const ENDPOINT_URL_US = `http://api.mixpanel.com/engage`
const ENDPOINT_URL_EU = `https://api-eu.mixpanel.com/engage`
const PROFILES_PER_REQUEST = 50


async function main(dataFile, isEU) {
    let ENDPOINT_URL = isEU ? ENDPOINT_URL_EU : ENDPOINT_URL_US;
    //LOAD data files
    let file = await readFilePromisified(dataFile).catch((e) => {
        console.error(`failed to load ${dataFile}... does it exist?\n`);
        process.exit(1);
    });


    //UNIFY
    //if it's already JSON, just use that
    let allData;
    try {
        allData = JSON.parse(file)
    } catch (e) {
        //it's probably NDJSON, so iterate over each line
        try {
            allData = file.split('\n').map(line => JSON.parse(line));
        } catch (e) {
            //if we don't have JSON or NDJSON... fail...
            console.log('failed to parse data... only valid JSON or NDJSON is supported by this script')
            console.log(e)
        }
    }

    console.log(`       parsed ${numberWithCommas(allData.length)} profiles from ${dataFile}`);

    //max 50 profiles per batch
    const batches = chunkForNumOfProfiles(allData, PROFILES_PER_REQUEST);



    //FLUSH
    console.log(`       sending ${numberWithCommas(allData.length)} profiles in ${numberWithCommas(batches.length)} batches`);
    let numRecordsImported = 0;
    for (let profileBatch of batches) {
        let result = await sendDataToMixpanel(profileBatch);

        // //some batches fail, for some reason?!?
        // if (result.status === 0) {
        //     debugger;
        // }
        // console.log(`   done ✅`)
        // console.log(`   mixpanel response:`)
        console.log(`           ${JSON.stringify(result)}`);
        //console.log('\n')
        numRecordsImported += profileBatch.length;
    }

    //FINISH
    //console.log(`   successfully imported ${numberWithCommas(numRecordsImported)} events`)
    return numRecordsImported;

    //HELPERS
    async function sendDataToMixpanel(batch) {
        //let authString = 'Basic ' + Buffer.from(auth.username + ':' + auth.password, 'binary').toString('base64');
        let url = `${ENDPOINT_URL}?verbose=1`
        let options = {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'

            },
            body: JSON.stringify(batch)
        }

        try {
            let req = await fetch(url, options);
            let res = await req.json();
            // console.log(`${JSON.stringify(res)}\n`)
            //some batches fail, for some reason?!?
            if (res.status === 0) {
                //debugger;
            }
            return res;

        } catch (e) {
            console.log(`   problem with request:\n${e}`)
        }
    }

    function chunkForNumOfProfiles(arrayOfProfiles, chunkSize) {
        return arrayOfProfiles.reduce((resultArray, item, index) => {
            const chunkIndex = Math.floor(index / chunkSize)

            if (!resultArray[chunkIndex]) {
                resultArray[chunkIndex] = [] // start a new chunk
            }

            resultArray[chunkIndex].push(item)

            return resultArray
        }, [])
    }

    function numberWithCommas(x) {
        return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
    }

}




export default main;