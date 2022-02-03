/*
some of the APIs toMixpanel calls have size and rate limits on /export APIs (like amplitude!)

for large sets of data, you will need to break a large date range into a smaller series of date ranges

this script is a one-off utility to do exactly that... 

usage:
node ampReplicator.js ./pathToConfig ./pathToWrite

where: 
./pathToConfig is a config with the full range of dates you need

./pathToWrite will create copies of the config with smaller date ranges (hourly)

these configs can be run serially to finish a large import....

the script builds a 'runAll.sh' bash script, and drops it in the ./pathToWrite directory to make that easir

have fun!
-AK

*/

//todo: https://www.msi.umn.edu/support/faq/how-can-i-use-gnu-parallel-run-lot-commands-parallel

const PARALLELISM = 5

import dayjs from 'dayjs';
import { writeFile, readFile } from 'fs/promises';
import { existsSync, mkdirSync } from 'fs'
import * as path from 'path'


async function main() {
    console.log('\nstarting replicator!\n')
    let cliArgs = process.argv;
    let configPath = path.resolve(cliArgs[2])
	let pathToWrite = path.resolve(cliArgs[3])	
    const userConfig = JSON.parse(await readFile(configPath))    
    console.log(`	found config @ ${configPath}\n`);
	let start = dayjs(userConfig.source.params.start_date);
	let end = dayjs(userConfig.source.params.end_date);
	let fullDateDelta = end.diff(start, 'd') * 24;
	let numOfConfigsNeeded = Math.ceil(fullDateDelta)
	
	let lastStart = start;
	let shellScript = `#!/usr/bin/env\nrm -rf ./logs/*\n`;
	let parallelsScript = ``
	createConfigs: for (let iterator = 0; iterator < numOfConfigsNeeded; iterator++) {
		let tempConfig = Object.assign({}, userConfig);
		tempConfig.source.params.start_date = lastStart.format('YYYY-MM-DDTHH');
		let newEnd = lastStart.add(1, 'h');
		tempConfig.source.params.end_date = newEnd.format('YYYY-MM-DDTHH');
		lastStart = newEnd.add(1, 'h');
		let newFileName = `${configPath.split('/').slice().pop().split('.json')[0]}-${iterator}.json`
		if (iterator % PARALLELISM === 0 && iterator !== 0) {
			shellScript += `wait\n`
		}
		shellScript += `node index.js ${path.resolve(`${pathToWrite}/${newFileName}`)} | tee -a ${path.resolve(`./logs/log-${newFileName}`)}.txt &\n`
		parallelsScript += `node index.js ${path.resolve(`${pathToWrite}/${newFileName}`)} | tee -a ${path.resolve(`./logs/log-${newFileName}`)}.txt\n`
		await writeFile(path.resolve(`${pathToWrite}/${newFileName}`), JSON.stringify(tempConfig, null, 2));
		
		
	}

	let finalShellScript = shellScript.trim()
	if (!finalShellScript.endsWith('wait')) {
		finalShellScript += `\nwait`
	}

	let finalParallelsScript = parallelsScript.trim();
	
	await writeFile(path.resolve(`${pathToWrite}/runAll.sh`), finalShellScript);
	await writeFile(path.resolve(`${pathToWrite}/commands.txt`), finalParallelsScript);
	console.log(`	created ${numOfConfigsNeeded} config files @ ${pathToWrite}/`);
	console.log(`\nyou can now use the generated bash script to run the entire import like this:`);
	console.log(`sh ${path.resolve(`${pathToWrite}/runAll.sh\n`)}`);
	console.log(`if 'parallel' is available, you may also do:`);
	console.log(`parallel --jobs 5 < ${path.resolve(`${pathToWrite}/commands.txt`)}`);


}


main();