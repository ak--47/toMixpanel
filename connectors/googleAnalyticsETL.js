import * as path from 'path';
import * as readline from 'readline'
import gaExtract from '../extract/googleAnalytics.js'
import gaTransform from '../transform/gaToMixpanel.js'
import { mapDefaults, mapUserProfiles, mapEvents } from '../transform/gaToMixpanel.js';
import sendEventsToMixpanel from '../load/sendEventsToMixpanel.js'
import sendProfilesToMixpanel from '../load/sendProfilesToMixpanel.js'
import { readdir, readFile } from 'fs/promises'
import {createReadStream, mkdirSync } from 'fs'
import { isGzip } from '../extract/googleAnalytics.js'
import { execSync } from 'child_process'


async function googleAnalyticsETL(config, directoryName) {
    const { bucket_name, keyFile, project_id } = config.source.params;
    let { options } = config.source
    if (!options) options = {};

    //FOR BIG DATA... GO LINE BY LINE...
    if (options.path_to_data) {
        console.log(`checking data at ${options.path_to_data}`);
        let listOfFiles = (await readdir(path.resolve(options.path_to_data))).map(file => path.resolve(`${options.path_to_data}/${file}`));
        for await (let filePath of listOfFiles) {
            //try to read the file
            try {
                let rawFile = await readFile(filePath);
                if (isGzip(rawFile)) {
                    console.log(`   unloading ${path.basename(filePath)}`);
                    let tempPath = `${path.dirname(filePath)}/tempFiles`;
                    execSync(`rm -rf ${tempPath}`);
                    mkdirSync(path.resolve(tempPath));
                    execSync(`gunzip -c ${filePath} > ${tempPath}/temp`);                   
                    const instream = createReadStream(`${tempPath}/temp`);
                    const rl = readline.createInterface({
                        input: instream,
                        crlfDelay: Infinity
                    });
                    let events = [];
                    let profiles = [];
                    
                    readEachLine: for await (const line of rl) {
                        let session = JSON.parse(line);
                        
                        //PROFILES
                        mapUserProfiles([session], config.destination.token).forEach(profile => profiles.push(profile));                        
                        
                        //EVENTS
                        mapEvents([session]).forEach(event => events.push(event));

                        //IF EVENTS > 1k ... flush and empty array
                        if (events.length > 1000) {
                            console.log(`       flushing ${smartCommas(events.length)} events and ${smartCommas(profiles.length)} profiles`)

                            //empty for gc
                            events.length = 0;
                            profiles.length = 0;
                        }
                    }


                } else {
                    //todo
                    
                }
            } catch (e) {
                debugger;
            }
            //JSON

            //TRANSFORM


            //SEND
        }


    } else {
        console.log('EXTRACT!\n')
        let extractedFiles = await gaExtract(project_id, bucket_name, keyFile, directoryName);

        console.log('\nTRANSFORM!\n')
        //NOTE TAKE OFF THE LAST PARAM SO THE DATES DON'T BUMP!
        let moveToPresent = config.source.options.move_data_to_present || false;
        let transform = await gaTransform(extractedFiles, `./savedData/${directoryName}`, config.destination.token, moveToPresent);


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



}

function smartCommas(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

export default googleAnalyticsETL;