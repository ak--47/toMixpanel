//deps
import dayjs from 'dayjs';
import * as path from 'path';

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

    let dates = {
        //co-erce the dates to amp format
        start: `${dayjs(config.source.params.start_date).format('YYYYMMDD')}T00`,
        end: `${dayjs(config.source.params.end_date).format('YYYYMMDD')}T23`
    }

    console.log('EXTRACT!\n')
    let extractedData = await amplitudeExtract(credentials, dates, directoryName, config.source.options['is EU?']);
    if (!extractedData) {
        return false;
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
        let eventsImported = await sendEventsToMixpanel(mixpanelCreds, eventDataFile, config.destination.options['is EU?']);
        totalEventsImported += eventsImported
    }

    console.log(`\nEVENT IMPORT FINISHED! imported ${smartCommas(totalEventsImported)} events\n`);

    console.log('LOAD!')
    console.log('   identity resolution:\n')
    //mergeTables
    let totalMergeTables = -1
    for await (let mergeTable of mergeTablePaths) {
        let mergeTablesImported = await sendEventsToMixpanel(mixpanelCreds, mergeTable, config.destination.options['is EU?']);
        totalMergeTables += mergeTablesImported
    }

    console.log(`\nIDENTITY RESOLVE FINISHED! imported ${smartCommas(totalMergeTables)} merged users\n`);

    console.log('LOAD!')
    console.log('   profiles:\n')
    let totalUsersImported = 0
    for await (let profileFile of profilePaths) {
        let profilesImported = await sendProfilesToMixpanel(profileFile, config.destination.options['is EU?'])
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