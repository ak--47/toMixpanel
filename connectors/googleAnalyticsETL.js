import * as path from 'path';
import gaExtract from '../extract/googleAnalytics.js'
import gaTransform from '../transform/gaToMixpanel.js'
import sendEventsToMixpanel from '../load/sendEventsToMixpanel.js'
import sendProfilesToMixpanel from '../load/sendProfilesToMixpanel.js'

async function googleAnalyticsETL(config, directoryName) {
    const { bucket_name, keyFile, project_id } = config.source.params;

    console.log('EXTRACT!\n')
    let extractedFiles = await gaExtract(project_id, bucket_name, keyFile, directoryName);

    console.log('\nTRANSFORM!\n')
    //NOTE TAKE OFF THE LAST PARAM SO THE DATES DON'T BUMP!
    let transform = await gaTransform(extractedFiles, `./savedData/${directoryName}`, config.destination.token);


    console.log('\nLOAD!\n')
    console.log('   events:\n')
    let { events: eventPaths, profiles: profilePaths, mergeTables: mergeTablePaths } = transform;
    let mixpanelCreds = {
        username: config.destination.service_account_user,
        password: config.destination.service_account_pass,
        project_id: config.destination.project_id
    }
    let totalEventsImported = 0;
    //events
    for await (let eventDataFile of eventPaths) {
        let eventsImported = await sendEventsToMixpanel(mixpanelCreds, eventDataFile, config.destination.options['is EU?']);
        totalEventsImported += eventsImported
    }

    console.log(`\nEVENT IMPORT FINISHED! imported ${smartCommas(totalEventsImported)} events\n`);

    console.log('LOAD!')
    console.log('   identity resolution:\n')
    //mergeTables
    let totalMergeTables = 0
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
    ${smartCommas(totalUsersImported)} profiles updated

    all data has been saved locally in ${path.resolve(directoryName)}
    you can rune 'npm run prune' to delete the data
    `)

    console.log(`you can now see your data in mixpanel!\nhttps://mixpanel.com/project/${config.destination.project_id}/`)

    process.exit(1)

}

function smartCommas(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

export default googleAnalyticsETL;