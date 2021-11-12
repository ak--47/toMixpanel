//deps
import { createWriteStream, readFile, readFileSync, writeFile, statSync, mkdirSync, existsSync, readdir } from 'fs';
import { promisify } from 'util';
import dayjs from 'dayjs';
import * as path from 'path';
import md5 from 'md5';
import { validate } from 'jsonschema';


const gaSchema = JSON.parse(readFileSync(path.resolve('./transform/gaSchema.json'), 'utf-8'));
const readFilePromisified = promisify(readFile);
const writeFilePromisified = promisify(writeFile);

async function main(listOfFilePaths, directory = "./savedData/foo/", mpToken, makeTimeCurrent = false) {

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
    walkAndTransform: for (let filePath of listOfFilePaths) {
        //load + parse file
        let fileNamePrefix = filePath.split('/').pop();
        console.log(`   processing ${fileNamePrefix}`);
        let file = await readFilePromisified(filePath);
        let json = JSON.parse(file.toString('utf-8').trim());

        //validate against GA schema: https://storage.googleapis.com/e-nor/visualizations/bigquery/ga360-schema.html
        let validator = validate(json, gaSchema).errors;
        if (validator.length > 0) {
            console.log(`       ${fileNamePrefix} does not conform to google analytics schema; skipping...`)
            console.log("\n");
            continue walkAndTransform;
        } else {
            console.log(`       ${fileNamePrefix} is valid google analytics data`)
        }

        //USER PROFILES
        writePath = path.resolve(`${dataPath}/profiles`);

        const mpUserProfiles = json.map(session => {
            let profile = {
                "$token": mpToken,
                "$distinct_id": ``,
                "$ip": 0,
                "$set": {}
            }
            //uuid
            let uuid = session.userId || session.fullVisitorId || session.visitorId || session.client_id || session.visitId || `not found!`
            profile.$distinct_id = uuid;

            let defaultProps = mapDefaults(session)

            profile.$set = { ...profile.$set, ...defaultProps };

            if (profile.$set.$latitude && profile.$set.$longitude) {
                profile.$latitude = profile.$set.$latitude;
                profile.$longitude = profile.$set.$longitude;
            }

            return profile;

        });

        totalProfileEntries += mpUserProfiles.length;
        console.log(`           transforming user profiles... (${smartCommas(mpUserProfiles.length)} profiles)`);

        //write file
        let profileFileName = path.resolve(`${writePath}/${fileNamePrefix.split('.')[0]}-profiles.json`)
        await writeFilePromisified(profileFileName, JSON.stringify(mpUserProfiles, null, 2));
        transformedPaths.profiles.push(profileFileName);


        //EVENTS
        writePath = path.resolve(`${dataPath}/events`);
        let mpEvents = [];

        //loop through sessions
        for (const session of json) {
            //each session gets a 'session start' and 'session end' event
            let uuid = session.userId || session.fullVisitorId || session.visitorId || session.client_id || session.visitId || `not found!`;
            let startTime = parseInt(session.visitStartTime);
            let endTime = parseInt(session.visitStartTime);
            let defaultProps = mapDefaults(session);
            let sessionSummary = session.totals;

            let eventStartTemplate = {
                "event": "session begins",
                "properties": {
                    "distinct_id": uuid,
                    "time": startTime,
                    "summary": sessionSummary,
                    ...defaultProps
                }
            }
            mpEvents.push(eventStartTemplate);

            for (const hit of session.hits) {
                let eventHitTemplate = {
                    "event": ``,
                    "properties": {
                        "distinct_id": uuid,
                        ...defaultProps
                    }
                }

                //time calc... hit time is in ms
                let eventTime;
                if (parseInt(hit.time) === 0) {
                    eventTime = startTime;

                } else {
                    eventTime = startTime + parseInt(hit.time)
                }
                //update end time
                endTime = eventTime;
                eventHitTemplate.properties.time = eventTime;

                //figure out event name!                
                let eventName;
                try {
                    if (hit.eventInfo) {
                        eventName = hit.eventInfo.eventAction;
                    } else {
                        throw Error();
                    }
                } catch (e) {
                    eventName = hit.type;
                }
                eventHitTemplate.event = eventName;

                //inline to help with adding simple props
                const addSimpleProps = (key, alias = null) => {
                    if (hit[key]) {
                        if (alias) {
                            eventHitTemplate.properties[alias] = hit[key]
                        } else {
                            eventHitTemplate.properties[key] = hit[key]
                        }
                    }
                }
                addSimpleProps('referrer', '$referrer');
                addSimpleProps('isEntrance');
                addSimpleProps('isExit');
                addSimpleProps('isInteraction');



                //inliner to help with adding nested props
                const addNestedProps = (props, alias = null) => {
                    if (Object.keys(props).length > 0) {
                        if (alias) {
                            eventHitTemplate.properties[alias] = props;
                        } else {
                            eventHitTemplate.properties = { ...eventHitTemplate.properties, ...props }
                        }
                    }
                }
                //test for various nest props
                hit.experiment ? addNestedProps(hit.experiment) : false;
                hit.product ? addNestedProps(hit.product, "products") : false;
                hit.transaction ? addNestedProps(hit.transaction) : false;
                hit.social ? addNestedProps(hit.social) : false;
                hit.page ? addNestedProps(hit.page) : false;
                hit.promotion ? addNestedProps(hit.promotion, "promotions") : false;
                hit.item ? addNestedProps(hit.item) : false;
                hit.appInfo ? addNestedProps(hit.appInfo) : false;
                hit.eventInfo ? addNestedProps(hit.eventInfo) : false;
                hit.customVariables ? addNestedProps(hit.customVariables) : false;
                hit.customDimensions ? addNestedProps(hit.customDimensions) : false;
                hit.customMetrics ? addNestedProps(hit.customMetrics) : false;
               
                mpEvents.push(eventHitTemplate);


            }

            let eventEndTemplate = {
                "event": "session ended",
                "properties": {
                    "distinct_id": uuid,
                    "time": endTime,
                    "summary": sessionSummary,
                    ...defaultProps
                }
            }
            mpEvents.push(eventEndTemplate);
        }

        //set insert_id on every event
        for (const event of mpEvents) {             
            let hash = md5(JSON.stringify(event));
            event.properties.$insert_id = hash;
        }
  

        //THIS IS JUST TO SEE SAMPLE DATA DO NOT USE
        if (makeTimeCurrent) {
            let oldest = dayjs.unix(mpEvents.slice(mpEvents.length - 1)[0].properties.time)
            let now = dayjs();
            let timeToAdd = now.diff(oldest, "seconds") - 172800;
            mpEvents.forEach((ev) => {
                ev.properties.time += timeToAdd;
            })
        }

        totalEventsTransformed += mpEvents.length
        console.log(`           transforming events... (${smartCommas(mpEvents.length)} events)`);

        //write file
        let eventsFileName = path.resolve(`${writePath}/${fileNamePrefix.split('.')[0]}-events.json`)
        await writeFilePromisified(eventsFileName, JSON.stringify(mpEvents, null, 2));
        transformedPaths.events.push(eventsFileName);

        //TODO MERGE!
        //create merge tables
        let mergeTable = [];
        writePath = path.resolve(`${dataPath}/mergeTables`);



        //     //transform events
        //     writePath = path.resolve(`${dataPath}/events`);
        //     let mpEvents = json.map((amplitudeEvent) => {
        //         let mixpanelEvent = {
        //             "event": amplitudeEvent.event_type,
        //             "properties": {
        //                 //prefer user_id, then device_id, then amplitude_id
        //                 "distinct_id": amplitudeEvent.user_id || amplitudeEvent.device_id || amplitudeEvent.amplitude_id.toString(),
        //                 "$device_id": amplitudeEvent.device_id,
        //                 "time": dayjs(amplitudeEvent.event_time).valueOf(),
        //                 "ip": amplitudeEvent.ip_address,
        //                 "$city": amplitudeEvent.city,
        //                 "$region": amplitudeEvent.region,
        //                 "mp_country_code": amplitudeEvent.country
        //             }

        //         }

        //         //get all custom props
        //         mixpanelEvent.properties = { ...amplitudeEvent.event_properties, ...mixpanelEvent.properties }

        //         //remove what we don't need
        //         delete amplitudeEvent.user_properties;
        //         delete amplitudeEvent.group_properties;
        //         delete amplitudeEvent.global_user_properties;
        //         delete amplitudeEvent.event_properties;
        //         delete amplitudeEvent.groups;
        //         delete amplitudeEvent.data;

        //         //fill in defaults & delete from amp data (if found)
        //         for (let ampMixPair of ampMixPairs) {
        //             if (amplitudeEvent[ampMixPair[0]]) {
        //                 mixpanelEvent.properties[ampMixPair[1]] = amplitudeEvent[ampMixPair[0]];
        //                 delete amplitudeEvent[ampMixPair[0]];
        //             }
        //         }

        //         //gather everything else
        //         mixpanelEvent.properties = { ...amplitudeEvent, ...mixpanelEvent.properties }

        //         //set insert_id
        //         let hash = md5(JSON.stringify(mixpanelEvent));
        //         mixpanelEvent.properties.$insert_id = hash;

        //         return mixpanelEvent
        //     });

        //     totalEventsTransformed += mpEvents.length
        //     console.log(`       transforming events... (${smartCommas(mpEvents.length)} events)`);

        //     //write file
        //     let eventsFileName = path.resolve(`${writePath}/${fileNamePrefix.split('.')[0]}-events.json`)
        //     await writeFilePromisified(eventsFileName, JSON.stringify(mpEvents, null, 2));
        //     transformedPaths.events.push(eventsFileName);

        //     //create merge tables
        //     let mergeTable = [];
        //     writePath = path.resolve(`${dataPath}/mergeTables`);

        //     for (let ampEvent of json) {
        //         // //pair device_id & user_id
        //         // if (ampEvent.device_id && ampEvent.user_id) {
        //         //     mergeTable.push({
        //         //         "event": "$merge",
        //         //         "properties": {
        //         //             "$distinct_ids": [
        //         //                 ampEvent.device_id,
        //         //                 ampEvent.user_id
        //         //             ]
        //         //         }
        //         //     });
        //         // }

        //         // //pair device_id & amplitude_id
        //         // if (ampEvent.device_id && ampEvent.amplitude_id) {
        //         //     mergeTable.push({
        //         //         "event": "$merge",
        //         //         "properties": {
        //         //             "$distinct_ids": [
        //         //                 ampEvent.device_id,
        //         //                 ampEvent.amplitude_id.toString()
        //         //             ]
        //         //         }
        //         //     })
        //         // };

        //         //pair user_id & amplitude_id
        //         if (ampEvent.user_id && ampEvent.amplitude_id) {
        //             mergeTable.push({
        //                 "event": "$merge",
        //                 "properties": {
        //                     "$distinct_ids": [
        //                         ampEvent.user_id,
        //                         ampEvent.amplitude_id.toString()
        //                     ]
        //                 }
        //             })
        //         };

        //     }

        //     //de-dupe the merge table
        //     let mergeTableDeDuped = Array.from(new Set(mergeTable.map(o => JSON.stringify(o))), s => JSON.parse(s));


        //     totalMergeTables += mergeTableDeDuped.length
        //     console.log(`       creating merge tables... (${smartCommas(mergeTableDeDuped.length)} entries)`);

        //     //write file
        //     let mergeTableFileName = path.resolve(`${writePath}/${fileNamePrefix.split('.')[0]}-mergeTable.json`)
        //     await writeFilePromisified(mergeTableFileName, JSON.stringify(mergeTableDeDuped, null, 2));
        //     transformedPaths.mergeTables.push(mergeTableFileName);
        //     console.log('\n')


        // }

        // //console.log(transformedPaths)
        // console.log(`transfomed ${smartCommas(totalProfileEntries)} profiles operations, ${smartCommas(totalEventsTransformed)} events, and ${smartCommas(totalMergeTables)} merge entries\n`)
        return transformedPaths
    }
}

//UTILITY METHODS

function getSessionSummary(session) {
    let summary = {};

}

function mapDefaults(session) {
    let props = {};
    //map default props
    //all props on the device key of GA session
    let GAmixDevicePairs = [
        ["browser", "$browser"],
        ["browserSize", "screen size"],
        ["browserVersion", "$browser_version"],
        ["deviceCategory", "device type"],
        ["mobileDeviceInfo", "$device"],
        ["mobileDeviceModel", "$model"],
        ["operatingSystem", "$os"],
        ["operatingSystemVersion", "$os_version"],
        ["mobileDeviceBranding", "$brand"],
        ["language", "language"],
        ["screenResolution", "screen size"]
    ];

    let GAmixLocationPairs = [
        ["continent", "continent"],
        ["subContinent", "sub continent"],
        ["country", "mp_country_code"],
        ["region", "$region"],
        ["metro", "dma"],
        ["city", "$city"],
        ["latitude", "$latitude"],
        ["longitude", "$longitude"]
    ]

    let GAmixAttributionPairs = [
        ["adContent", "utm_content"],
        ["adWordsClickInfo", "ad words info"],
        ["campaign", "utm_campaign"],
        ["campaignCode", "utm_term"],
        ["isTrueDirect", "is true direct?"],
        ["keyword", "utm_keyword"],
        ["medium", "utm_medium"],
        ["referralPath", "$referrer"],
        ["source", "utm_source"]
    ]


    //include defaults, if they exist:
    //DEVICE INFO:
    for (let GAmixDevicePair of GAmixDevicePairs) {
        if (session.device[GAmixDevicePair[0]]) {
            props[GAmixDevicePair[1]] = session.device[GAmixDevicePair[0]]
        }
    }

    //LOCATION INFO:
    for (let GAmixLocationPair of GAmixLocationPairs) {
        if (session.geoNetwork[GAmixLocationPair[0]]) {
            props[GAmixLocationPair[1]] = session.geoNetwork[GAmixLocationPair[0]]
        }
    }

    //ATTIRBUTION INFO:        
    for (let GAmixAttributionPair of GAmixAttributionPairs) {
        if (session.trafficSource[GAmixAttributionPair[0]]) {
            props[GAmixAttributionPair[1]] = session.trafficSource[GAmixAttributionPair[0]]
        }
    }
    try {
        //explicit check for long/latitude
        if (session.geoNetwork.latitude && session.geoNetwork.longitude) {
            props.$latitude = session.geoNetwork.latitude
            props.$longitude = session.geoNetwork.longitude
        }
    } catch (e) {}
    try {
        if (session.channelGrouping) {
            props["UTM Channel"] = session.channelGrouping;
        }
    } catch (e) {

    }

    return props;
}

function smartCommas(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

export default main;