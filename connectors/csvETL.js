//deps
import dayjs from 'dayjs';
import * as path from 'path';
import Papa from 'papaparse';
import { readFile, writeFile } from 'fs/promises';
import sendEventsToMixpanel from '../load/sendEventsToMixpanel.js'
import sendProfilesToMixpanel from '../load/sendProfilesToMixpanel.js'

async function main(config, directoryName) {
    console.log(`EXTRACT`);
    let file;
    try {
        console.log(`	reading ${config.source.params.filePath}`)
        file = await (await readFile(config.source.params.filePath)).toString('utf-8')

    } catch (e) {
        console.log(`		error: could not load ${config.source.params.filePath} (does it exist?)`)
    }

    //parse CSV as json
    let data = Papa.parse(file, { "header": true }).data;

    console.log(`		found ${smartCommas(data.length)} events`);

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
        event[cols.time_col] = dayjs(event[cols.time_col]).unix();

		//ignore cols
		if (config.source.options.ignore_cols.length >= 1) {
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

        events.push(transformedEvent);
		
		//do profiles
        if (config.source.options.create_profiles) {
            let profile = {
                "$token": config.destination.token,
                "$distinct_id": transformedEvent.properties.distinct_id,
                "$ip": "0",
                "$ignore_time": true,
                "$set": {
                    "uuid": transformedEvent.properties.distinct_id
                }
            }
            profiles.push(profile);
        }

    })

    let uniqueProfiles = profiles.filter((v, i, a) => a.findIndex(t => (t.$distinct_id === v.$distinct_id)) === i)

    console.log(`	transformed ${smartCommas(events.length)} events`);
    console.log(`	created ${smartCommas(uniqueProfiles.length)} profiles`);

    let eventFilePath = `${path.resolve("./savedData/" + directoryName)}/events.json`
    let profileFilePath = `${path.resolve("./savedData/" + directoryName)}/profiles.json`


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
    let totalEventsImported = -1;
    let eventsImported = await sendEventsToMixpanel(mixpanelCreds, eventFilePath, config.destination.options['is EU?']);
    totalEventsImported += eventsImported
    console.log(`\nEVENT IMPORT FINISHED! imported ${smartCommas(totalEventsImported)} events\n`);

    console.log('   profiles:\n')
    let totalUsersImported = 1
    let profilesImported = await sendProfilesToMixpanel(profileFilePath, config.destination.options['is EU?'])
    totalUsersImported += profilesImported

    console.log(`\nPROFILES FINISHED! imported ${smartCommas(totalUsersImported)} profiles\n`);

    console.log(`\nSUMMARY:`)
    console.log(`
    ${smartCommas(totalEventsImported)} events imported
    ${smartCommas(totalUsersImported)} profiles updated

    all data has been saved locally in ${path.resolve(directoryName)}
    you can rune 'npm run prune' to delete the data
    `)

    console.log(`you can now see your data in mixpanel!\nhttps://mixpanel.com/project/${config.destination.project_id}/`)
    process.exit(1)

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


export default main;