// toMixpanel is your one-stop-shop ETL to get data from various sources... into Mixpanel!
// by AK
// ak@mixpanel.com


//deps
import { statSync, mkdirSync, existsSync, readdir } from 'fs';
import { readFile } from 'fs/promises';
import { promisify } from 'util'
import * as path from 'path';
import dayjs from 'dayjs';

//supported ETLS
import amplitudeExtract from './extract/amplitude.js'
import amplitudeTransform from './transform/amplitudeToMixpanel.js'
import sendEventsToMixpanel from './load/sendEventsToMixpanel.js'
import sendProfilesToMixpanel from './load/sendProfilesToMixpanel.js'

async function main() {
    console.log('starting up!')
    //config
    const userConfig = await readFile('./config.json')
    let config;
    try {
        config = JSON.parse(userConfig)
    } catch (e) {
        console.error('derp... invalid json on your config\nhere is an example:\n')
        const configExample = await readFile('./configExample.json');
        console.log(JSON.stringify(JSON.parse(configExample), null, 2))
        process.exit(-1)
    }






    //create a root folder for everything
    const now = dayjs().format('YYYY-MM-DD HH.MM.ss A');
    const directoryName = `${config.source.name} ${now}`;

    mkdirSync(path.resolve(`./savedData/${directoryName}/`));

    const directory = path.resolve(directoryName)

    //determine which etl to run
    switch (config.source.name.toLowerCase()) {
        case 'amplitude':
            console.log(`lets migrate data from ${config.source.name} to Mixpanel!\n\n`);
            amplitudeETL();
            break;
        case y:
            // code block
            break;
        default:
            console.log('could not determine data source')
    }


    async function amplitudeETL() {
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
        let extractedData = await amplitudeExtract(credentials, dates, directoryName);

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
            let eventsImported = await sendEventsToMixpanel(mixpanelCreds, eventDataFile);
            totalEventsImported += eventsImported
        }

        console.log(`\nEVENT IMPORT FINISHED! imported ${smartCommas(totalEventsImported)} events\n`);

        console.log('LOAD!')
        console.log('   identity resolution:\n')
        //mergeTables
        let totalMergeTables = -1
        for await (let mergeTable of mergeTablePaths) {
            let mergeTablesImported = await sendEventsToMixpanel(mixpanelCreds, mergeTable);
            totalMergeTables += mergeTablesImported
        }

        console.log(`\nIDENTITY RESOLVE FINISHED! imported ${smartCommas(totalMergeTables)} merged users\n`);

        console.log('LOAD!')
        console.log('   profiles:\n')
        let totalUsersImported = 0
        for await (let profileFile of profilePaths) {
            let profilesImported = await sendProfilesToMixpanel(profileFile)
            totalUsersImported += profilesImported
        }

        console.log(`\nPROFILES FINISHED! imported ${smartCommas(totalUsersImported)} profiles\n`);

        console.log(`hooray... you have data in mixpanel\ncheck it out: https://mixpanel.com/project/${config.destination.project_id}/`)

        process.exit(1)

    }
}

main()

//logging stuffs
function smartCommas(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}