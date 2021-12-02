import * as path from 'path';
import * as readline from 'readline'
import { mapUserProfiles, mapEvents } from '../transform/gaToMixpanel.js';
import sendEventsToMixpanel from '../load/sendEventsToMixpanel.js'
import sendProfilesToMixpanel from '../load/sendProfilesToMixpanel.js'
import { readdir, readFile } from 'fs/promises'
import { createReadStream, mkdirSync } from 'fs'
import { isGzip } from '../extract/googleAnalytics.js'
import { execSync } from 'child_process'


async function main(config, directoryName) {
    let mixpanelCreds = {
        username: config.destination.service_account_user,
        password: config.destination.service_account_pass,
        project_id: config.destination.project_id
    }
    let totalEventsImported = 0;
    let totalProfilesImported = 0;
    console.log(`checking data at ${config.source.options.path_to_data}`);

    let listOfFiles = (await readdir(path.resolve(config.source.options.path_to_data))).map(file => path.resolve(`${config.source.options.path_to_data}/${file}`));
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
                        console.log(`           flushing! ${path.basename(filePath)}`)

                        await sendEventsToMixpanel(mixpanelCreds, events, config.destination.options['is EU?'], true);
                        await sendProfilesToMixpanel(profiles, config.destination.options['is EU?'], true);

                        //empty for gc
                        totalEventsImported += events.length
                        totalProfilesImported += profiles.length
                        events.length = 0;
                        profiles.length = 0;
                    }
                }


            } else {
                //todo

            }
        } catch (e) {
            console.log(`something failed`)
            console.log(e)
            process.exit(1)
        }
    }
    console.log(`finished ${smartCommas(totalEventsImported)} events + ${smartCommas(totalProfilesImported)} profiles`);
    process.exit()
}


export default main;