#!/usr/bin/env node
// toMixpanel is your one-stop-shop ETL to get data from various sources... into Mixpanel!
// by AK
// ak@mixpanel.com

//deps
import { statSync, mkdirSync, existsSync, readdir } from 'fs';
import { readFile } from 'fs/promises';
import { promisify } from 'util'
import * as path from 'path';
import dayjs from 'dayjs';
import { execSync } from 'child_process'

//connectors
import amplitudeETL from './connectors/amplitudeETL.js'
import googleAnalyticsETL from './connectors/googleAnalyticsETL.js'
import csvETL from './connectors/csvETL.js'
import mixpanelETL from './connectors/mixpanelETL.js'

//global now!
global.nowTime = Date.now();


async function main() {
    console.time("main");
    console.log('\nstarting up!\n')
    //figure out where config is
    let cliArgs = process.argv;
    let configFromArgs = cliArgs.filter(argument => argument.includes('.json'));
    let configPath;
    if (configFromArgs.length > 0) {
        configPath = configFromArgs[0];
    } else {
        configPath = './config.json';
    }
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
    console.log(`found config @ ${configPath}\n`);

    //create a root folder for everything
    const now = dayjs().format('YYYY-MM-DD HH.MM.ss.SSS A');
    const randomNum = getRandomInt(420);
    let directoryName = `${config.source.name} ${now} ${randomNum}`;
    try {
        if (config.source.options.path_to_data) {
            directoryName = path.resolve(`./${config.source.options.path_to_data}`)
        } else {
            mkdirSync(path.resolve(`./savedData/${directoryName}/`));
        }
    } catch (error) {
        mkdirSync(path.resolve(`./savedData/${directoryName}/`));
    }

    console.log(`data dir is:`);
    console.log(path.resolve(`./savedData/${directoryName}/\n`));

    //determine which etl to run
    switch (config.source.name.toLowerCase()) {
        case 'amplitude':
            console.log(`lets migrate data from ${config.source.name} to Mixpanel!\n\n`);
            await amplitudeETL(config, directoryName);
            cleanUp()
            break;
        case 'googleanalytics':
            console.log(`lets migrate data from ${config.source.name} to Mixpanel!\n\n`);
            await googleAnalyticsETL(config, directoryName);
            cleanUp()
            break;
        case 'csv':
            console.log(`lets migrate ${config.source.name} data to Mixpanel!\n\n`);
            await csvETL(config, directoryName);
            cleanUp()
            break;
        case 'mixpanel':
            console.log(`lets migrate ${config.source.name} data ... to mixpanel!`)
            await mixpanelETL(config, directoryName);
            cleanUp();
        default:
            console.log('could not determine data source')
    }

    function cleanUp() {
        //if save local copy is disabled, remove saved files
        if (!config.source.options.save_local_copy) {
            console.log(`\ndeleting temp data\n`)
            execSync(`rm -rf ${escapeForShell(path.resolve(`./savedData/${directoryName}`))}`);
        } else {
            console.log(`\nall data has been saved locally in ${path.resolve(directoryName)}\nyou can run 'npm run prune' to delete the data if you don't need it anymore
            `)
        }

        console.log(`you can now see your data in mixpanel!\nhttps://mixpanel.com/project/${config.destination.project_id}/`)
        console.log('\n')
        console.metrics();
        console.timeEnd("main")
        process.exit()

    }

    //utils
    function escapeForShell(arg) {
        return `'${arg.replace(/'/g, `'\\''`)}'`;
    }

    function getRandomInt(max) {
        return Math.floor(Math.random() * max);
    }

}

console.metrics = function() {
    const used = process.memoryUsage();
    for (let key in used) {
        console.log(`Memory: ${key} ${Math.round(used[key] / 1024 / 1024 * 100) / 100} MB`);
    }
}

main()