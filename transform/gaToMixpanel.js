//deps
import { createWriteStream, readFile, readFileSync, writeFile, statSync, mkdirSync, existsSync, readdir } from 'fs';
import { promisify } from 'util';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
dayjs.extend(utc);
import * as path from 'path';
import md5 from 'md5';
import { validate } from 'jsonschema';
import {fileURLToPath} from 'url';
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);


const gaSchema = JSON.parse(readFileSync(path.resolve(path.join(__dirname, '/gaSchema.json')), 'utf-8'));
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
        const mpUserProfiles = mapUserProfiles(json, mpToken);
        totalProfileEntries += mpUserProfiles.length;
        console.log(`           transforming user profiles... (${smartCommas(mpUserProfiles.length)} profiles)`);
        let profileFileName = path.resolve(`${writePath}/${fileNamePrefix.split('.')[0]}-profiles.json`)
        await writeFilePromisified(profileFileName, JSON.stringify(mpUserProfiles, null, 2));
        transformedPaths.profiles.push(profileFileName);


        //EVENTS
        writePath = path.resolve(`${dataPath}/events`);
        const mpEvents = mapEvents(json, makeTimeCurrent);
        totalEventsTransformed += mpEvents.length
        console.log(`           transforming events... (${smartCommas(mpEvents.length)} events)`);

        //write file
        let eventsFileName = path.resolve(`${writePath}/${fileNamePrefix.split('.')[0]}-events.json`)
        await writeFilePromisified(eventsFileName, JSON.stringify(mpEvents, null, 2));
        transformedPaths.events.push(eventsFileName);


        // //TODO: create merge tables for all possible uuids
        // let mergeTable = [];
        // writePath = path.resolve(`${dataPath}/mergeTables`);
        // let allSessionsWithIdentifiers = json.map(session => {
        //     return {
        //         userId: session.userId,
        //         fullVisitorId: session.fullVisitorId,
        //         visitorId: session.visitorId,
        //         clientId: session.client_id,
        //         visitId: session.visitId
        //     }
        // });
        
        return transformedPaths
    }
}

//UTILITY METHODS

export function mapUserProfiles(json, mpToken) {
    //every "line" in GA data represents a session; for each line we $set a user profile
    return json.map(session => {
        let profile = {
            "$token": mpToken,
            "$distinct_id": ``,
            "$ip": 0,
            "$set": {}
        }
        //resolve distinct_id as any one of these props
        let uuid = session.userId || session.fullVisitorId || session.visitorId || session.client_id || session.visitId || ``
        profile.$distinct_id = uuid;

        //gather default props
        let defaultProps = mapDefaults(session)
        profile.$set = { ...profile.$set, ...defaultProps };

        //if $lat and $long is given, move it to the top of the object
        if (profile.$set.$latitude && profile.$set.$longitude) {
            profile.$latitude = profile.$set.$latitude;
            profile.$longitude = profile.$set.$longitude;
        }

        return profile;

    });
}

export function mapEvents(json, makeTimeCurrent = false) {
    let mpEvents = [];

    //loop through sessions
    for (const session of json) {
        //resolve distinct_id as any one of these props
        let uuid = session.userId || session.fullVisitorId || session.visitorId || session.client_id || session.visitId || ``;

        //session time is in seconds; convert to ms
        let startTime = parseInt(session.visitStartTime) * 1000;
        let endTime = parseInt(session.visitStartTime) * 1000;
        let defaultProps = mapDefaults(session);
        let sessionSummary = session.totals;

        //each session gets a 'session begins' and 'session ends' event
        let eventStartTemplate = {
            "event": "session begins",
            "properties": {
                "distinct_id": uuid,
                "time": startTime,
                "summary": sessionSummary,
                "$source": `ga360toMixpanel (by AK)`,
                ...defaultProps
            }
        }
        mpEvents.push(eventStartTemplate);

        //session events are in 'hits'
        for (const hit of session.hits) {
            let eventHitTemplate = {
                "event": ``,
                "properties": {
                    "distinct_id": uuid,
                    "$source": `ga360toMixpanel (by AK)`,
                    ...defaultProps
                }
            }

            //time calc for each "hit" is in ms, offset from session begins
            let eventTime;
            if (parseInt(hit.time) === 0) {
                eventTime = startTime + 1000;

            } else {
                eventTime = startTime + parseInt(hit.time)
            }
            eventHitTemplate.properties.time = eventTime;

            //always update end time
            endTime = eventTime;

            //resolve event's name
            let eventName;
            try {
                if (hit.eventInfo) {
                    if (hit.eventInfo.eventAction.toLowerCase() !== "na" && hit.eventInfo.eventAction !== "") {
                        eventName = hit.eventInfo.eventAction;
                    }
                    else {
                        eventName = hit.eventInfo.eventCategory;
                    }
                } else {
                    throw new Error();
                }
            } catch (e) {
                eventName = hit.type;
            }

            if (!eventName) {
                console.log(`could not resolve event name for:\n${JSON.stringify(hit, null, 2)}`)
            }

            eventHitTemplate.event = eventName;

            //helper for standard props on hits
            const addSimpleProps = (key, alias = null) => {
                if (hit[key]) {
                    if (alias) {
                        eventHitTemplate.properties[alias] = hit[key]
                    } else {
                        eventHitTemplate.properties[key] = hit[key]
                    }
                }
            }

            //helper for standard hit props that are nested
            const addNestedProps = (props, alias = null) => {

                if (Object.keys(props).length > 0) {
                    if (alias) {
                        eventHitTemplate.properties[alias] = props;
                    } else {
                        eventHitTemplate.properties = { ...eventHitTemplate.properties, ...props }
                    }
                }
            }

            //helper for custom dimensions
            //note: we do not get labels (keys) from GA raw data; just an index
            const addCustomMetrics = (customDims, prefix, suffix) => {
                let tempObj = {};
                customDims.forEach((dimension) => {
                    //only set dimensions that have values
                    if (dimension.value.toLowerCase() !== "na" && dimension.value !== "") {
                        tempObj[`${prefix} #${dimension.index} (${suffix})`] = dimension.value
                    }
                })
                if (Object.keys(tempObj).length > 0) {
                    eventHitTemplate.properties = { ...eventHitTemplate.properties, ...tempObj }
                }
            }

            //standard hit props
            addSimpleProps('referrer', '$referrer');
            addSimpleProps('isEntrance');
            addSimpleProps('isExit');
            addSimpleProps('isInteraction');

            //test for standard nested props
            hit.experiment ? addNestedProps(hit.experiment) : false;
            hit.product ? addNestedProps(hit.product, "products") : false;
            hit.transaction ? addNestedProps(hit.transaction) : false;
            hit.social ? addNestedProps(hit.social) : false;
            hit.page ? addNestedProps(hit.page) : false;
            hit.promotion ? addNestedProps(hit.promotion, "promotions") : false;
            hit.item ? addNestedProps(hit.item) : false;
            hit.appInfo ? addNestedProps(hit.appInfo) : false;
            hit.eventInfo ? addNestedProps(hit.eventInfo) : false;

            //test for custom metrics
            hit.customVariables ? addCustomMetrics(hit.customVariables, `variable`, eventName) : false;
            hit.customDimensions ? addCustomMetrics(hit.customDimensions, `dimension`, eventName) : false;
            hit.customMetrics ? addCustomMetrics(hit.customMetrics, `metric`, eventName) : false;

            mpEvents.push(eventHitTemplate);

        }

        //bump session ends time one second to ensure proper sequencing
        endTime += 1000

        let eventEndTemplate = {
            "event": "session ends",
            "properties": {
                "distinct_id": uuid,
                "time": endTime,
                "summary": sessionSummary,
                "$source": `ga360toMixpanel (by AK)`,
                ...defaultProps
            }
        }
        mpEvents.push(eventEndTemplate);
    }

    //set an $insert_id on every event
    for (const event of mpEvents) {
        let hash = md5(JSON.stringify(event));
        event.properties.$insert_id = hash;
    }


    //bump events into the present (if the data is really old)
    if (makeTimeCurrent) {
        let oldest = dayjs(mpEvents.slice(mpEvents.length - 1)[0].properties.time)
        let now = dayjs();
        let timeToAdd = now.diff(oldest, "ms") - (345600 * 1000);
        mpEvents.forEach((ev) => {
            ev.properties.time += timeToAdd;
        })
    }

    return mpEvents;
}

export function mapDefaults(session) {
    //map default GA props to default mp Props
    let props = {};
    
    //device pairs
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

    //location pairs
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

    //attribution pairs
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

    //loop through all pairs; if they exist, append them

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
        //check for $lat and $long
        if (session.geoNetwork.latitude && session.geoNetwork.longitude) {
            props.$latitude = session.geoNetwork.latitude
            props.$longitude = session.geoNetwork.longitude
        }
    } catch (e) {}
    
    try {
        //check for channel groupings
        if (session.channelGrouping) {
            props["UTM Channel"] = session.channelGrouping;
        }
    } catch (e) {}

    return props;
}

function smartCommas(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

export default main;