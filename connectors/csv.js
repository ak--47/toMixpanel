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
            fileContents = await (await readFile(file)).toString('utf-8').trim()


        } catch (e) {
            console.log(`	error: could not load ${file} (does it exist?)`)
            console.log(e.message)
            console.log(`\n`)
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
            console.log(e)
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

			else {
				//if it doesnt, make one
				transformedEvent.properties.$insert_id = md5(JSON.stringify(transformedEvent))
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
                        "uuid": transformedEvent.properties.distinct_id,
						"$name": transformedEvent.properties.distinct_id
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


//md5
function md5(inputString) {
    var hc="0123456789abcdef";
    function rh(n) {var j,s="";for(j=0;j<=3;j++) s+=hc.charAt((n>>(j*8+4))&0x0F)+hc.charAt((n>>(j*8))&0x0F);return s;}
    function ad(x,y) {var l=(x&0xFFFF)+(y&0xFFFF);var m=(x>>16)+(y>>16)+(l>>16);return (m<<16)|(l&0xFFFF);}
    function rl(n,c)            {return (n<<c)|(n>>>(32-c));}
    function cm(q,a,b,x,s,t)    {return ad(rl(ad(ad(a,q),ad(x,t)),s),b);}
    function ff(a,b,c,d,x,s,t)  {return cm((b&c)|((~b)&d),a,b,x,s,t);}
    function gg(a,b,c,d,x,s,t)  {return cm((b&d)|(c&(~d)),a,b,x,s,t);}
    function hh(a,b,c,d,x,s,t)  {return cm(b^c^d,a,b,x,s,t);}
    function ii(a,b,c,d,x,s,t)  {return cm(c^(b|(~d)),a,b,x,s,t);}
    function sb(x) {
        var i;var nblk=((x.length+8)>>6)+1;var blks=new Array(nblk*16);for(i=0;i<nblk*16;i++) blks[i]=0;
        for(i=0;i<x.length;i++) blks[i>>2]|=x.charCodeAt(i)<<((i%4)*8);
        blks[i>>2]|=0x80<<((i%4)*8);blks[nblk*16-2]=x.length*8;return blks;
    }
    var i,x=sb(inputString),a=1732584193,b=-271733879,c=-1732584194,d=271733878,olda,oldb,oldc,oldd;
    for(i=0;i<x.length;i+=16) {olda=a;oldb=b;oldc=c;oldd=d;
        a=ff(a,b,c,d,x[i+ 0], 7, -680876936);d=ff(d,a,b,c,x[i+ 1],12, -389564586);c=ff(c,d,a,b,x[i+ 2],17,  606105819);
        b=ff(b,c,d,a,x[i+ 3],22,-1044525330);a=ff(a,b,c,d,x[i+ 4], 7, -176418897);d=ff(d,a,b,c,x[i+ 5],12, 1200080426);
        c=ff(c,d,a,b,x[i+ 6],17,-1473231341);b=ff(b,c,d,a,x[i+ 7],22,  -45705983);a=ff(a,b,c,d,x[i+ 8], 7, 1770035416);
        d=ff(d,a,b,c,x[i+ 9],12,-1958414417);c=ff(c,d,a,b,x[i+10],17,     -42063);b=ff(b,c,d,a,x[i+11],22,-1990404162);
        a=ff(a,b,c,d,x[i+12], 7, 1804603682);d=ff(d,a,b,c,x[i+13],12,  -40341101);c=ff(c,d,a,b,x[i+14],17,-1502002290);
        b=ff(b,c,d,a,x[i+15],22, 1236535329);a=gg(a,b,c,d,x[i+ 1], 5, -165796510);d=gg(d,a,b,c,x[i+ 6], 9,-1069501632);
        c=gg(c,d,a,b,x[i+11],14,  643717713);b=gg(b,c,d,a,x[i+ 0],20, -373897302);a=gg(a,b,c,d,x[i+ 5], 5, -701558691);
        d=gg(d,a,b,c,x[i+10], 9,   38016083);c=gg(c,d,a,b,x[i+15],14, -660478335);b=gg(b,c,d,a,x[i+ 4],20, -405537848);
        a=gg(a,b,c,d,x[i+ 9], 5,  568446438);d=gg(d,a,b,c,x[i+14], 9,-1019803690);c=gg(c,d,a,b,x[i+ 3],14, -187363961);
        b=gg(b,c,d,a,x[i+ 8],20, 1163531501);a=gg(a,b,c,d,x[i+13], 5,-1444681467);d=gg(d,a,b,c,x[i+ 2], 9,  -51403784);
        c=gg(c,d,a,b,x[i+ 7],14, 1735328473);b=gg(b,c,d,a,x[i+12],20,-1926607734);a=hh(a,b,c,d,x[i+ 5], 4,    -378558);
        d=hh(d,a,b,c,x[i+ 8],11,-2022574463);c=hh(c,d,a,b,x[i+11],16, 1839030562);b=hh(b,c,d,a,x[i+14],23,  -35309556);
        a=hh(a,b,c,d,x[i+ 1], 4,-1530992060);d=hh(d,a,b,c,x[i+ 4],11, 1272893353);c=hh(c,d,a,b,x[i+ 7],16, -155497632);
        b=hh(b,c,d,a,x[i+10],23,-1094730640);a=hh(a,b,c,d,x[i+13], 4,  681279174);d=hh(d,a,b,c,x[i+ 0],11, -358537222);
        c=hh(c,d,a,b,x[i+ 3],16, -722521979);b=hh(b,c,d,a,x[i+ 6],23,   76029189);a=hh(a,b,c,d,x[i+ 9], 4, -640364487);
        d=hh(d,a,b,c,x[i+12],11, -421815835);c=hh(c,d,a,b,x[i+15],16,  530742520);b=hh(b,c,d,a,x[i+ 2],23, -995338651);
        a=ii(a,b,c,d,x[i+ 0], 6, -198630844);d=ii(d,a,b,c,x[i+ 7],10, 1126891415);c=ii(c,d,a,b,x[i+14],15,-1416354905);
        b=ii(b,c,d,a,x[i+ 5],21,  -57434055);a=ii(a,b,c,d,x[i+12], 6, 1700485571);d=ii(d,a,b,c,x[i+ 3],10,-1894986606);
        c=ii(c,d,a,b,x[i+10],15,   -1051523);b=ii(b,c,d,a,x[i+ 1],21,-2054922799);a=ii(a,b,c,d,x[i+ 8], 6, 1873313359);
        d=ii(d,a,b,c,x[i+15],10,  -30611744);c=ii(c,d,a,b,x[i+ 6],15,-1560198380);b=ii(b,c,d,a,x[i+13],21, 1309151649);
        a=ii(a,b,c,d,x[i+ 4], 6, -145523070);d=ii(d,a,b,c,x[i+11],10,-1120210379);c=ii(c,d,a,b,x[i+ 2],15,  718787259);
        b=ii(b,c,d,a,x[i+ 9],21, -343485551);a=ad(a,olda);b=ad(b,oldb);c=ad(c,oldc);d=ad(d,oldd);
    }
    return rh(a)+rh(b)+rh(c)+rh(d);
}

export default main;