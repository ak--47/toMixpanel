//deps
import dayjs from 'dayjs';
import * as path from 'path';
import Papa from 'papaparse';
import { readFile, writeFile, appendFile, readdir } from 'fs/promises';
import fs from 'fs';
import sendEventsToMixpanel from '../load/sendEventsToMixpanel.js'
import sendProfilesToMixpanel from '../load/sendProfilesToMixpanel.js'

async function main(config, directoryName) {
    console.log(`EXTRACT`);
    let files = [];
    let totalEventsImported = 0;
    let totalUsersImported = 0;
    let counter = 1;

    let isDirectory = fs.lstatSync(config.source.params.filePath).isDirectory();
    if (isDirectory) {
        console.log(`${config.source.params.filePath} is a directory`)
        let allFiles = await readdir(config.source.params.filePath);
        for (let file of allFiles) {
            files.push(path.resolve(`${config.source.params.filePath}/${file}`))
        }
        console.log(`found ${smartCommas(files.length)} files\n\n`)

    } else {
        files.push(config.source.params.filePath)
    }


    loopCSVfiles: for await (const file of files) {
        let fileContents;
        try {
            console.log(`	reading ${file}`)
            fileContents = await (await readFile(file)).toString('utf-8')


        } catch (e) {
            console.log(`	error: could not load ${file} (does it exist?)`)
            console.log(`\n`)
            console.log(e.message)
            continue loopCSVfiles;
        }

        let data;
        try {
            //parse CSV as json
            let parsed = Papa.parse(fileContents, { "header": true });
            if (parsed.data.length === 0 || parsed.errors.length > 0) {
                throw new Error();
            }
            data = parsed.data;

            console.log(`   found ${smartCommas(data.length)} events`);
        } catch (e) {
            console.log(`   error: could not parse ${file} as CSV`)
            console.log(`\n`)
            continue loopCSVfiles;
        }

        console.log(`\nTRANSFORM`)

        let cols = config.source.params;

        //core transformation
        const events = [];
        const profiles = [];
        data.forEach((event) => {
            //setup event
            let transformedEvent = {};
            transformedEvent.event = event[cols.event_name_col]
            transformedEvent.properties = {};
            delete event[cols.event_name_col]

            //fix time
            let eventTime = event[cols.time_col];
            if (isNum(eventTime)) {
                //unix ms is usually 13+ digits
                if (eventTime.toString().length >= 13) {
                    event[cols.time_col] = dayjs(Number(eventTime)).unix()
                } else {
                    event[cols.time_col] = dayjs.unix(Number(eventTime)).unix()
                }
            } else {
                event[cols.time_col] = dayjs(eventTime).unix();
            }


            //ignore cols
            if (config.source.options?.ignore_cols?.length >= 1) {
                for (let header of config.source.options.ignore_cols) {
                    delete event[header];
                }
            }

            //rename keys
            renameKeys(transformedEvent.properties, event, "distinct_id", cols.distinct_id_col);
            if (cols.distinct_id_col !== "distinct_id") {
                delete event[cols.distinct_id_col];
            }
            renameKeys(transformedEvent.properties, event, "time", cols.time_col);
            if (cols.time_col !== "time") {
                delete event[cols.time_col];
            }

            //use insert_id if it exists
            if (cols.insert_id_col) {
                renameKeys(transformedEvent.properties, event, "$insert_id", cols.insert_id_col);
            }

            //tag :)
            transformedEvent.properties.$source = `csvtoMixpanel (by AK)`
            if (config.source?.options?.tag) {
                transformedEvent.properties['import-tag'] = config.source?.options?.tag
            }

            events.push(transformedEvent);

            //do profiles
            if (config.source.options?.create_profiles) {
                let profile = {
                    "$token": config.destination.token,
                    "$distinct_id": transformedEvent.properties.distinct_id,
                    "$ip": "0",
                    "$ignore_time": true,
                    "$set": {
                        "uuid": transformedEvent.properties.distinct_id
                    }
                }

                if (config.source?.options?.tag) {
                    profile.$set['import-tag'] = config.source?.options?.tag
                }

                profiles.push(profile);
            }

        })

        let uniqueProfiles = profiles.filter((v, i, a) => a.findIndex(t => (t.$distinct_id === v.$distinct_id)) === i)

        console.log(`	transformed ${smartCommas(events.length)} events`);
        console.log(`	created ${smartCommas(uniqueProfiles.length)} profiles`);

        let eventFilePath = `${path.resolve("./savedData/" + directoryName)}/events-${counter}.json`
        let profileFilePath = `${path.resolve("./savedData/" + directoryName)}/profiles-${counter}.json`

        //write copies
        await writeFile(eventFilePath, JSON.stringify(events, null, 2));
        await writeFile(profileFilePath, JSON.stringify(uniqueProfiles, null, 2));


        console.log(`\nLOAD`)
        console.log('   events:\n')
        let mixpanelCreds = {
            username: config.destination.service_account_user,
            password: config.destination.service_account_pass,
            project_id: config.destination.project_id
        }

        let eventsImported = await sendEventsToMixpanel(mixpanelCreds, eventFilePath, config.destination.options['is EU?']);
        totalEventsImported += eventsImported
        console.log(`\nEVENT IMPORT FINISHED! imported ${smartCommas(totalEventsImported)} events\n`);

        console.log('   profiles:\n')

        let profilesImported = await sendProfilesToMixpanel(profileFilePath, config.destination.options['is EU?'])
        totalUsersImported += profilesImported

        console.log(`\nPROFILES FINISHED! imported ${smartCommas(totalUsersImported)} profiles\n`);
        counter++
    }

    if (totalEventsImported === 0 && totalUsersImported === 0) {
        console.log(`\ncould not find any valid CSV files in ${config.source.params.filePath}\n`)
        process.exit()
    }

    console.log(`\nSUMMARY:`)
    console.log(`
    ${smartCommas(totalEventsImported)} events imported
    ${smartCommas(totalUsersImported)} profiles updated`)

}


function renameKeys(newObject, oldObject, newKey, oldKey) {
    return delete Object.assign(newObject, oldObject, {
        [newKey]: oldObject[oldKey]
    })[oldKey];
}

//logging stuffs
function smartCommas(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function isNum(val) {
    return !isNaN(val)
}


export default main;