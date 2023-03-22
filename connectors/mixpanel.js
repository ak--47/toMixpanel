import { execSync } from 'child_process'
import { writeFile, readFile } from 'fs/promises';
import dayjs from 'dayjs';
import _ from 'lodash';
import * as path from 'path';
import sendOther from '../load/sendOther.js';
import mpImport from 'mixpanel-import'


const ENDPOINT_URL_US_EVENTS = `https://data.mixpanel.com/api/2.0/export`
const ENDPOINT_URL_EU_EVENTS = `https://data-eu.mixpanel.com/api/2.0/export`

const ENDPOINT_URL_US_PEOPLE = `https://mixpanel.com/api/2.0/engage`
const ENDPOINT_URL_EU_PEOPLE = `https://eu.mixpanel.com/api/2.0/engage`



async function mixpanelETL(config, directoryName) {
    //make sure curl is installed
    try {
        execSync(`which curl`).toString();
    } catch (e) {
        console.error(`\nERROR:\n     this script requires 'curl' to be installed\n     you can get it here: https://curl.se/dlwiz/\n\n`)
        process.exit()
    }

    let ENDPOINT_URL_EVENTS = config.source?.options?.["is EU?"] ? ENDPOINT_URL_EU_EVENTS : ENDPOINT_URL_US_EVENTS;
    let ENDPOINT_URL_PEOPLE = config.source?.options?.["is EU?"] ? ENDPOINT_URL_EU_PEOPLE : ENDPOINT_URL_US_PEOPLE;

    //for auth
    let sourceCreds = {
        token: config.source.params.token,
        secret: config.source.params.secret
    }
    let auth = "Basic " + Buffer.from(`${sourceCreds.secret}:`).toString('base64')

    let mixpanelCreds = {
        acct: config.destination.service_account_user,
        pass: config.destination.service_account_pass,
        project: config.destination.project_id,
        token: config.destination.token
    }

    //date looper... create array of arrays with start & end dates
    let { start_date, end_date } = config.source.params;
    start_date = dayjs(start_date).format('YYYY-MM-DD');
    end_date = dayjs(end_date).format('YYYY-MM-DD')
    // const datePairs = [];
    // let lastStart = dayjs(start_date);
    // let end = dayjs(end_date)
    // let lastEnd = lastStart.add(1, 'd');
    // datePairs.push([lastStart.format('YYYY-MM-DD'), lastEnd.format('YYYY-MM-DD')]);

    // do {
    //     lastStart = lastStart.add(2, 'd');
    //     lastEnd = lastEnd.add(2, 'd');
    //     if (lastEnd.isAfter(end)) {
    //         datePairs.push([end.format('YYYY-MM-DD'), end.format('YYYY-MM-DD')])
    //         break;
    //     } else {
    //         datePairs.push([lastStart.format('YYYY-MM-DD'), lastEnd.format('YYYY-MM-DD')]);

    //     }

    // } while (!end.isSame(lastEnd));
    let eventsImported = 0;
    let peopleImported = 0;
    let region = (config.destination?.options['is EU?'] ? `EU` : `US`);

    if (config.source?.options?.doEvents) {
        console.log(`\nextracting + loading EVENTS from ${start_date} to ${end_date}\n`)
        //the ETL loop for events

        let fileName = `${start_date}_to_${end_date}.jsonl`
        let file = path.resolve(`./savedData/${directoryName}/${fileName}`)
        let queryString = `from_date=${start_date}&to_date=${end_date}`


        //add where and event clauses if specific
        if (config.source?.options?.where) {
            queryString += `&where=${encode(config.source?.options?.where)}`
        }
        if (config.source?.options?.event && config.source?.options?.event?.length > 0) {
            queryString += `&event=${encode(JSON.stringify(config.source?.options?.event))}`
        }
        console.log(`   hitting /export for ${start_date} to ${end_date}`)
        let curlForData = `curl -sS --request GET --url '${ENDPOINT_URL_EVENTS}?${queryString}' --header 'Accept: text/plain' --header 'Authorization: ${auth}' --output ${escapeForShell(file)}`
        let fetchData = execSync(curlForData);		

        if (config.destination.name.toLowerCase() !== 'mixpanel') {
            eventsImported += await sendOther(config.destination.name.toLowerCase(), config, file, `event`)
        } else {
            let req = await mpImport(mixpanelCreds, file, { recordType: `event`, logs: true, region, recordsPerBatch: config?.destination?.options?.recordsPerBatch || 2000 });
            eventsImported += req.results.totalRecordCount
        }

        if (!config.source?.options?.save_local_copy) {
            console.log(`   deleting ${fileName}`)
            execSync(`rm ${escapeForShell(file)}`)
        }

    }




    if (config.source?.options?.doPeople) {
        console.log(`\nextracting + loading PEOPLE\n`)
        //ETL for people
        let iterations = 0;
        let fileName = `people-${iterations}.json`
		let tempFileName = `people-${iterations}-source.json`
        let file = path.resolve(`./savedData/${directoryName}/${fileName}`)
		let tempFilePath = path.resolve(`./savedData/${directoryName}/${tempFileName}`)
        let curlForData = `curl -sS --request POST --url ${ENDPOINT_URL_PEOPLE} --header 'Accept: application/json' --header 'Authorization: ${auth}' --header 'Content-Type: application/x-www-form-urlencoded' --data include_all_users=false  --output ${escapeForShell(tempFilePath)}`;
		let proc = execSync(curlForData)
        let peopleRes = JSON.parse(await readFile(tempFilePath, "utf-8"));
        //reshape peopleData
        let peopleData = peopleRes.results.map(function(person) {
            return {
                "$token": config.destination.token,
                "$distinct_id": person.$distinct_id,
                "$ignore_time": true,
                "$ip": 0,
                "$set": {
                    ...person.$properties
                }
            }
        });

        await writeFile(file, JSON.stringify(peopleData));
        if (config.destination.name.toLowerCase() !== 'mixpanel') {
            peopleImported += await sendOther(config.destination.name.toLowerCase(), config, file, `user`, peopleData)
        } else {
            let req = await mpImport(mixpanelCreds, file, { recordType: `user`, logs: true, region });
            peopleImported += req.results.totalRecordCount
        }

        execSync(`rm -rf ${escapeForShell(file)}`)
        let lastPage = peopleRes.page;
        let lastSession = peopleRes.session_id;
        let lastNumResults = peopleRes.results.length;
        let lastPageSize = peopleRes.page_size;
        while (lastNumResults >= lastPageSize) {
            lastPage++
            iterations++
            fileName = `people-${iterations}.json`
            file = path.resolve(`./savedData/${directoryName}/${fileName}`)
			tempFileName = `people-${iterations}-source.json`
			tempFilePath = path.resolve(`./savedData/${directoryName}/${tempFileName}`)

            curlForData = `curl -sS --request POST --url ${ENDPOINT_URL_PEOPLE} --header 'Accept: application/json' --header 'Authorization: ${auth}' --header 'Content-Type: application/x-www-form-urlencoded' --data include_all_users=false --data session_id=${lastSession} --data page=${lastPage} --output ${escapeForShell(tempFilePath)}`;
			proc = execSync(curlForData)
            peopleRes = JSON.parse(await readFile(tempFilePath, "utf-8"));
            //reshape peopleData
            peopleData = peopleRes.results.map(function(person) {
                return {
                    "$token": config.destination.token,
                    "$distinct_id": person.$distinct_id,
                    "$ignore_time": true,
                    "$ip": 0,
                    "$set": {
                        ...person.$properties
                    }
                }
            });

            await writeFile(file, JSON.stringify(peopleData))
            if (config.destination.name.toLowerCase() !== 'mixpanel') {
                peopleImported += await sendOther(config.destination.name.toLowerCase(), config, file, `user`, peopleData)
            } else {
                let req = await mpImport(mixpanelCreds, file, { recordType: `user`, logs: true, region, recordsPerBatch: config?.destination?.options?.recordsPerBatch || 2000 || 2000 });
                peopleImported += req.results.totalRecordCount
            }
            if (!config.source?.options?.save_local_copy) {
                console.log(`deleting ${file}`)
                execSync(`rm -rf ${escapeForShell(file)}`)
            }
            lastPage = peopleRes.page;
            lastNumResults = peopleRes.results.length;

        }
    }


    console.log(`\nSUMMARY:`)
    console.log(`${smartCommas(eventsImported)} total events imported`)
    console.log(`${smartCommas(peopleImported)} total people imported`)



}

//utils
function escapeForShell(arg) {
    return `'${arg.replace(/'/g, `'\\''`)}'`;
}

function sleep(milliseconds) {
    return new Promise((resolve) => setTimeout(resolve, milliseconds))
}

function smartCommas(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function encode(str) {
    return encodeURIComponent(str).
    // Note that although RFC3986 reserves "!", RFC5987 does not,
    // so we do not need to escape it
    replace(/['()]/g, escape). // i.e., %27 %28 %29
    replace(/\*/g, '%2A').
    // The following are not required for percent-encoding per RFC5987, 
    //  so we can allow for a little better readability over the wire: |`^
    replace(/%(?:7C|60|5E)/g, unescape);
}

export default mixpanelETL;