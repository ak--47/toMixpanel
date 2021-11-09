// toMixpanel is your one-stop-shop ETL to get data from various sources... into Mixpanel!
// by AK
// ak@mixpanel.com


//deps
import { statSync, mkdirSync, existsSync, readdir } from 'fs';
import { readFile } from 'fs/promises';
import { promisify } from 'util'
import * as path from 'path';
import dayjs from 'dayjs';

//connectors
import amplitudeETL from './connectors/amplitudeETL.js'
import googleAnalyticsETL from './connectors/googleAnalyticsETL.js'



async function main() {
    console.log('starting up!')
    //figure out where config is
    let cliArgs = process.argv;
    let configFromArgs = cliArgs.filter(argument => argument.includes('.json'));
    let configPath;
    if (configFromArgs.length > 0) {
        configPath = configFromArgs[0];
    } else {
        configPath = './config.json';
    }
    console.log(`found ${configPath}`);
    const userConfig = await readFile(configPath)
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
    //const directory = path.resolve(directoryName)

    //determine which etl to run
    switch (config.source.name.toLowerCase()) {
        case 'amplitude':
            console.log(`lets migrate data from ${config.source.name} to Mixpanel!\n\n`);
            amplitudeETL(config, directoryName);
            break;
        case 'googleanalytics':
            console.log(`lets migrate data from ${config.source.name} to Mixpanel!\n\n`);
            googleAnalyticsETL(config, directoryName);
            // code block
            break;
        default:
            console.log('could not determine data source')
    }


}

main()