import { execSync } from 'child_process'
import dayjs from 'dayjs';
import _ from 'lodash';
import * as path from 'path';
import sendEventsToMixpanel from '../load/sendEventsToMixpanel.js'

const ENDPOINT_URL_US = `https://data.mixpanel.com/api/2.0/export`
const ENDPOINT_URL_EU = `https://data-eu.mixpanel.com/api/2.0/export`

async function mixpanelETL(config, directoryName) {
    let ENDPOINT_URL = config.source?.options?.["is EU?"] ? ENDPOINT_URL_EU : ENDPOINT_URL_US;
    //for auth
    let sourceCreds = {
        token: config.source.params.token,
        secret: config.source.params.secret
    }
    let auth = "Basic " + Buffer.from(`${sourceCreds.secret}:`).toString('base64')

    let mixpanelCreds = {
        username: config.destination.service_account_user,
        password: config.destination.service_account_pass,
        project_id: config.destination.project_id
    }

    //date looper... create array of arrays with start & end dates
    let { start_date, end_date } = config.source.params;
    const datePairs = [];
    let lastStart = dayjs(start_date);
    let end = dayjs(end_date)
    let lastEnd = lastStart.add(1, 'd');
    datePairs.push([lastStart.format('YYYY-MM-DD'), lastEnd.format('YYYY-MM-DD')]);

    do {
        lastStart = lastStart.add(2, 'd');
        lastEnd = lastEnd.add(2, 'd');
        if (lastEnd.isAfter(end)) {
            datePairs.push([end.format('YYYY-MM-DD'), end.format('YYYY-MM-DD')])
            break;
        } else {
            datePairs.push([lastStart.format('YYYY-MM-DD'), lastEnd.format('YYYY-MM-DD')]);

        }

    } while (!end.isSame(lastEnd));
    let eventsImported = 0
    
    for await (const datePair of datePairs) {
        let [start, end] = datePair
        let fileName = `${start}_to_${end}.json`
        let file = path.resolve(`./savedData/${directoryName}/${fileName}`)
        let queryString = `from_date=${start}&to_date=${end}&where=${config.source?.options?.where}`
        let curlForData = `curl --request GET --url '${ENDPOINT_URL}?${queryString}' --header 'Accept: text/plain' --header 'Authorization: ${auth}' > ${escapeForShell(file)}`
        execSync(curlForData);
        eventsImported += await sendEventsToMixpanel(mixpanelCreds, file, config.destination?.options['is EU?']);
        execSync(`rm ${escapeForShell(file)}`)
    }


console.log(`\nSUMMARY:`)
console.log(`${smartCommas(eventsImported)} total events imported`)
    // console.log(`
    // ${smartCommas(totalEventsImported)} events imported
    // ${smartCommas(totalMergeTables)} users merged
    // ${smartCommas(totalUsersImported)} profiles updated`)



}

//utils
function escapeForShell(arg) {
    return `'${arg.replace(/'/g, `'\\''`)}'`;
}

//logging stuffs
function smartCommas(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}



export default mixpanelETL;