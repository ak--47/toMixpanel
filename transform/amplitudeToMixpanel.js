//deps
import { createWriteStream, readFile, writeFile, statSync, mkdirSync, existsSync, readdir } from 'fs';
import { promisify } from 'util';
import dayjs from 'dayjs';
import * as path from 'path';
import md5 from 'md5';

const readFilePromisified = promisify(readFile);
const writeFilePromisified = promisify(writeFile);

async function main(listOfFilePaths, directory = "./savedData/foo/", mpToken) {

    try {
        mkdirSync(path.resolve(`${directory}/transformed`))
        mkdirSync(path.resolve(`${directory}/transformed/events`))
        mkdirSync(path.resolve(`${directory}/transformed/profiles`))
        mkdirSync(path.resolve(`${directory}/transformed/mergeTables`))
    } catch (e) {
        console.log(e)
    }

    let dataPath = path.resolve(`${directory}/transformed`);
    let writePath; //write target
    let totalEventsTransformed = 0;
    let totalProfileEntries = 0;
    let totalMergeTables = 0;
    let transformedPaths = {
        events: [],
        profiles: [],
        mergeTables: []
    }


    //walk each file    
    for (let filePath of listOfFilePaths) {
        let fileNamePrefix = filePath.split('/').pop();
        console.log(`   processing ${fileNamePrefix}`)
        let file = await readFilePromisified(filePath);
        let jsonl = file.toString('utf-8').trim().split('\n');
        let json = jsonl.map(line => JSON.parse(line));

        //mapping amp default to mp defauls
        //https://developers.amplitude.com/docs/identify-api
        //https://help.mixpanel.com/hc/en-us/articles/115004613766-Default-Properties-Collected-by-Mixpanel
        let ampMixPairs = [
            ["app_version", "$app_version_string"],
            ["os_name", "$os"],
            ["os_name", "$browser"],
            ["os_version", "$os_version"],
            ["device_brand", "$brand"],
            ["device_manufacturer", "$manufacturer"],
            ["device_model", "$model"],
            ["region", "$region"],
            ["city", "$city"]
        ]

        //transform user props
        writePath = path.resolve(`${dataPath}/profiles`);
        let mpUserProfiles = json.filter((amplitudeEvent) => {
                return Object.keys(amplitudeEvent.user_properties).length !== 0;
            })
            .map((amplitudeEvent) => {
                let profile = {
                    "$token": mpToken,
                    "$distinct_id": amplitudeEvent.user_id,
                    "$ip": amplitudeEvent.ip_address,
                    "$set": amplitudeEvent.user_properties
                }

                //include defaults, if they exist
                for (let ampMixPair of ampMixPairs) {
                    if (amplitudeEvent[ampMixPair[0]]) {
                        profile.$set[ampMixPair[1]] = amplitudeEvent[ampMixPair[0]]
                    }
                }

                return profile;

            });


        totalProfileEntries += mpUserProfiles.length;
        console.log(`       transforming user profiles... (${smartCommas(mpUserProfiles.length)} profiles)`);

        //write file
        let profileFileName = path.resolve(`${writePath}/${fileNamePrefix.split('.')[0]}-profiles.json`)
        await writeFilePromisified(profileFileName, JSON.stringify(mpUserProfiles, null, 2));
        transformedPaths.profiles.push(profileFileName);

        //transform events
        writePath = path.resolve(`${dataPath}/events`);
        let mpEvents = json.map((amplitudeEvent) => {
            let mixpanelEvent = {
                "event": amplitudeEvent.event_type,
                "properties": {
                    //prefer user_id, then device_id, then amplitude_id
                    "distinct_id": amplitudeEvent.user_id || amplitudeEvent.device_id || amplitudeEvent.amplitude_id.toString(),
                    "$device_id": amplitudeEvent.device_id,
                    "time": dayjs(amplitudeEvent.event_time).valueOf(),
                    "ip": amplitudeEvent.ip_address,
                    "$city": amplitudeEvent.city,
                    "$region": amplitudeEvent.region,
                    "mp_country_code": amplitudeEvent.country
                }

            }

            //get all custom props
            mixpanelEvent.properties = { ...amplitudeEvent.event_properties, ...mixpanelEvent.properties }

            //remove what we don't need
            delete amplitudeEvent.user_properties;
            delete amplitudeEvent.group_properties;
            delete amplitudeEvent.global_user_properties;
            delete amplitudeEvent.event_properties;
            delete amplitudeEvent.groups;
            delete amplitudeEvent.data;

            //fill in defaults & delete from amp data (if found)
            for (let ampMixPair of ampMixPairs) {
                if (amplitudeEvent[ampMixPair[0]]) {
                    mixpanelEvent.properties[ampMixPair[1]] = amplitudeEvent[ampMixPair[0]];
                    delete amplitudeEvent[ampMixPair[0]];
                }
            }

            //gather everything else
            mixpanelEvent.properties = { ...amplitudeEvent, ...mixpanelEvent.properties }

            //set insert_id
            let hash = md5(JSON.stringify(mixpanelEvent));
            mixpanelEvent.properties.$insert_id = hash;

            return mixpanelEvent
        });

        totalEventsTransformed += mpEvents.length
        console.log(`       transforming events... (${smartCommas(mpEvents.length)} events)`);

        //write file
        let eventsFileName = path.resolve(`${writePath}/${fileNamePrefix.split('.')[0]}-events.json`)
        await writeFilePromisified(eventsFileName, JSON.stringify(mpEvents, null, 2));
        transformedPaths.events.push(eventsFileName);

        //create merge tables
        let mergeTable = [];
        writePath = path.resolve(`${dataPath}/mergeTables`);

        for (let ampEvent of json) {
            // //pair device_id & user_id
            // if (ampEvent.device_id && ampEvent.user_id) {
            //     mergeTable.push({
            //         "event": "$merge",
            //         "properties": {
            //             "$distinct_ids": [
            //                 ampEvent.device_id,
            //                 ampEvent.user_id
            //             ]
            //         }
            //     });
            // }

            // //pair device_id & amplitude_id
            // if (ampEvent.device_id && ampEvent.amplitude_id) {
            //     mergeTable.push({
            //         "event": "$merge",
            //         "properties": {
            //             "$distinct_ids": [
            //                 ampEvent.device_id,
            //                 ampEvent.amplitude_id.toString()
            //             ]
            //         }
            //     })
            // };

            //pair user_id & amplitude_id
            if (ampEvent.user_id && ampEvent.amplitude_id) {
                mergeTable.push({
                    "event": "$merge",
                    "properties": {
                        "$distinct_ids": [
                            ampEvent.user_id,
                            ampEvent.amplitude_id.toString()
                        ]
                    }
                })
            };

        }

        //de-dupe the merge table
        let mergeTableDeDuped = Array.from(new Set(mergeTable.map(o => JSON.stringify(o))), s => JSON.parse(s));


        totalMergeTables += mergeTableDeDuped.length
        console.log(`       creating merge tables... (${smartCommas(mergeTableDeDuped.length)} entries)`);

        //write file
        let mergeTableFileName = path.resolve(`${writePath}/${fileNamePrefix.split('.')[0]}-mergeTable.json`)
        await writeFilePromisified(mergeTableFileName, JSON.stringify(mergeTableDeDuped, null, 2));
        transformedPaths.mergeTables.push(mergeTableFileName);
        console.log('\n')


    }

    //console.log(transformedPaths)
    console.log(`transfomed ${smartCommas(totalProfileEntries)} profiles operations, ${smartCommas(totalEventsTransformed)} events, and ${smartCommas(totalMergeTables)} merge entries\n`)
    return transformedPaths
}

function smartCommas(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}


export default main;