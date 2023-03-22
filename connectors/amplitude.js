//deps
import dayjs from 'dayjs';
import * as path from 'path';
import { readdir } from 'fs/promises';
import mpImport from 'mixpanel-import'

//scripts
import amplitudeExtract from '../extract/amplitude.js'
import amplitudeTransform from '../transform/amplitudeToMixpanel.js'
import sendEventsToMixpanel from '../load/sendEventsToMixpanel.js'
import sendProfilesToMixpanel from '../load/sendProfilesToMixpanel.js'


async function amplitudeETL(config, directoryName) {
    //for auth
    let credentials = {
        apiKey: config.source.params.api_key,
        apiSecret: config.source.params.api_secret
    }

    let { start_date, end_date } = config.source.params;
    //co-erce the dates to amp format
    let dates = {
        start: dayjs(start_date).format('YYYYMMDDTHH'),
        end: dayjs(end_date).format('YYYYMMDDTHH')
    }

    let extractedData;
    if (config.source.options?.path) {
        //path to data already specified
        let filesPath = path.resolve(config.source.options?.path)
        console.log(`local path specified: ${filesPath}`)
        let files = (await readdir(filesPath)).map(fileLoc => path.resolve(`${filesPath}/${fileLoc}`))
        console.log(`found ${files.length} files`)
        extractedData = files;
    } else {

        console.log('EXTRACT!\n')
        extractedData = await amplitudeExtract(credentials, dates, directoryName, config.source.options['is EU?']);
        if (!extractedData) {
            return false;
        }
    }
    console.log('TRANSFORM!\n')
    let transformedData = await amplitudeTransform(extractedData, `./savedData/${directoryName}`, config.destination.token);

    console.log('LOAD!')
    console.log('   events:\n')
    let eventsDir = path.resolve(`./savedData/${directoryName}/transformed/events`);
    let usersDir = path.resolve(`./savedData/${directoryName}/transformed/profiles`);
    let mergeDir = path.resolve(`./savedData/${directoryName}/transformed/mergeTables`);

    let creds  = {
        acct: config.destination.service_account_user,
        pass: config.destination.service_account_pass,
        project: config.destination.project_id,
        token: config.destination.token

    }
    let region = (config.destination?.options['is EU?'] ? `EU` : `US`);

    let importedEvents = await mpImport(creds, eventsDir, {recordType: `event`, logs: true, region, recordsPerBatch: config?.destination?.options?.recordsPerBatch || 2000 });    
    let imporedProfiles = await mpImport(creds, usersDir, {recordType: `user`, logs: true, region, recordsPerBatch: config?.destination?.options?.recordsPerBatch || 2000 });
    let importedMergeTables = await mpImport(creds, mergeDir, {recordType: `event`, logs: true, region, recordsPerBatch: config?.destination?.options?.recordsPerBatch || 2000 });

    console.log(`\nSUMMARY:`)
    console.log(`
    ${smartCommas(importedEvents.results?.totalRecordCount)} events imported
    ${smartCommas(imporedProfiles.results?.totalRecordCount)} users merged
    ${smartCommas(importedMergeTables.results?.totalRecordCount)} profiles updated`)



}

//logging stuffs
function smartCommas(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

export default amplitudeETL;