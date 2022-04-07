import { createReadStream } from 'fs';
import split from 'split'
import _ from 'lodash';
import axios from "axios";


async function main(vendor, config, dataFile, recordType = `event`, batch) {
    let imported;
    if (vendor === `amplitude`) {
        imported = await ampSend(config, dataFile, recordType, batch)
    }

    return imported;
}


async function ampSend(config, dataFile, recordType, batch) {
    let allData = [];
    async function streamJSON(dataFile) {
        return new Promise((resolve, reject) => {
            createReadStream(dataFile)
                .pipe(split(JSON.parse, null, { trailing: false }))
                .on('data', function(obj) {
                    allData.push(obj)
                })
                .on('end', function() {
                    resolve()
                })
        })
    }
    await streamJSON(dataFile)

    const creds = {
        apiKey: config.destination.token,
        secret: config.destination.secret
    }

    //quick and dirty transforms
    if (recordType === `event`) {
        allData = allData.map((sourceEvent) => {
            let src = JSON.parse(JSON.stringify(sourceEvent));
            let tempObj = {};
            tempObj.user_id = src.properties.distinct_id;
            tempObj.device_id = src.properties.distinct_id;
            tempObj.event_type = src.event;
            tempObj.time = src.properties.time * 1000;
            tempObj.insert_id = src.properties.$insert_id;
            delete src.event;
            delete src.properties.time;
            delete src.properties.$insert_id;
			delete src.properties.distinct_id;
            tempObj.event_properties = { ...src.properties }
            return tempObj;
        })

        await amplitudeFlush(creds, allData, `events`)		
        return allData.length

    }


    if (recordType === `user`) {
        allData = batch.map((user) => {
            let src = JSON.parse(JSON.stringify(user));
            let tempObj = {};
            tempObj.user_id = src.$distinct_id;
            delete src.$distinct_id;
            tempObj.user_properties = { ...src.$set }
            return tempObj;
        })
        
		await amplitudeFlush(creds, allData, `users`)
        return allData.length

    }
}

async function amplitudeFlush(creds, data, type) {
    console.log(`       sending ${numberWithCommas(data.length)} ${type}`);
	// https://developers.amplitude.com/docs/http-api-v2#parameters
    if (type === "events") {
        const chunks = _.chunk(data, 2000);
        let options = {
            url: `https://api2.amplitude.com/2/httpapi`,
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            data: {
                api_key: creds.apiKey
            }
        }
        //console.log(`\namplitude events:`)
        for (const chunk of chunks) {
            options.data.events = chunk;
			await sleep(2000) // HACK!
            await axios(options).then(function(response) {
			
				// console.log(response.data);
            }).catch(function(error) {
                // uh oh!
                console.log(error);
            })
        }
    }


    //https://developers.amplitude.com/docs/identify-api#keys-for-the-identification-argument
    else if (type === "users") {
        const chunks = _.chunk(data, 1000);
        //console.log(`\namplitude users:`)
        for (const chunk of chunks) {
            let options = {
                url: `https://api2.amplitude.com/identify`,
                method: 'POST',
                data: `api_key=${creds.apiKey}&identification=`
            }
            options.data += JSON.stringify(chunk)
            await axios(options).then(function(response) {
                console.log(response.data);
            }).catch(function(error) {
                // uh oh!
                console.log(error);
            })
        }

    }


    function numberWithCommas(x) {
        return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
    }
}


function sleep (milliseconds) {
	console.log(`sleeping 2 seconds (amp rate limit)`)
	return new Promise((resolve) => setTimeout(resolve, milliseconds))
  }

export default main