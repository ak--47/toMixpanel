//deps
import dayjs from 'dayjs';
import * as path from 'path';
import { readdir } from 'fs/promises';

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
    let { events: eventPaths, profiles: profilePaths, mergeTables: mergeTablePaths } = transformedData;
    let mixpanelCreds = {
        username: config.destination.service_account_user,
        password: config.destination.service_account_pass,
        project_id: config.destination.project_id
    }
    let totalEventsImported = -1;
    //events
    for await (let eventDataFile of eventPaths) {
        let eventsImported;
        try {
            eventsImported = await sendEventsToMixpanel(mixpanelCreds, eventDataFile, config.destination.options['is EU?']);
        } catch (e) {

        }
        totalEventsImported += eventsImported
    }

    console.log(`\nEVENT IMPORT FINISHED! imported ${smartCommas(totalEventsImported)} events\n`);

    console.log('LOAD!')
    console.log('   identity resolution:\n')
    //mergeTables
    let totalMergeTables = -1
    for await (let mergeTable of mergeTablePaths) {
        let mergeTablesImported
        try {
            mergeTablesImported = await sendEventsToMixpanel(mixpanelCreds, mergeTable, config.destination.options['is EU?']);
        } catch (e) {

        }
        totalMergeTables += mergeTablesImported
    }

    console.log(`\nIDENTITY RESOLVE FINISHED! imported ${smartCommas(totalMergeTables)} merged users\n`);

    console.log('LOAD!')
    console.log('   profiles:\n')
    let totalUsersImported = 0
    for await (let profileFile of profilePaths) {
        let profilesImported;
        try {
            profilesImported = await sendProfilesToMixpanel(profileFile, config.destination.options['is EU?'])
        } catch (e) {

        }
        totalUsersImported += profilesImported
    }

    console.log(`\nPROFILES FINISHED! imported ${smartCommas(totalUsersImported)} profiles\n`);

    console.log(`\nSUMMARY:`)
    console.log(`
    ${smartCommas(totalEventsImported)} events imported
    ${smartCommas(totalMergeTables)} users merged
    ${smartCommas(totalUsersImported)} profiles updated`)



}

//logging stuffs
function smartCommas(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

export default amplitudeETL;