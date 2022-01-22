/*
some of the APIs toMixpanel calls have size and rate limits on /export APIs (like amplitude!)

for large sets of data, you will need to break a large date range into a smaller series of date ranges

this script is a one-off utility to do exactly that... 

usage:
node replicator.js ./pathToConfig --chunk N ./pathToWrite

where: 
./pathToConfig is a config with the full range of dates you need

--chunk N is the number of days in each export

./pathToWrite will create copies of the config with smaller date ranges

these configs can be run serially to finish a large import....

the script builds a 'runAll.sh' bash script, and drops it in the ./pathToWrite directory to make that easir

have fun!
-AK

*/

import dayjs from 'dayjs';
import { writeFile, readFile } from 'fs/promises';
import { existsSync, mkdirSync } from 'fs'
import * as path from 'path'


async function main() {
    console.log('\nstarting replicator!\n')
    let cliArgs = process.argv;
    let configPath = path.resolve(cliArgs[2])
	let pathToWrite = path.resolve(cliArgs[5])
	let dayChunks = Number(cliArgs[4]) - 1   
    const userConfig = JSON.parse(await readFile(configPath))    
    console.log(`	found config @ ${configPath}\n`);
	let start = dayjs(userConfig.source.params.start_date);
	let end = dayjs(userConfig.source.params.end_date);
	let fullDateDelta = end.diff(start, 'd');
	let numOfConfigsNeeded = Math.ceil(fullDateDelta/dayChunks);
	
	let lastStart = start;
	let shellScript = `#!/usr/bin/env\nrm log.txt;\ntouch log.txt;\n`;
	createConfigs: for (let iterator = 0; iterator < numOfConfigsNeeded; iterator++) {
		let tempConfig = Object.assign({}, userConfig);
		tempConfig.source.params.start_date = lastStart.format('YYYY-MM-DD');
		let newEnd = lastStart.add(dayChunks, 'd');
		tempConfig.source.params.end_date = newEnd.format('YYYY-MM-DD');
		lastStart = newEnd.add(1, 'd');
		let newFileName = `${configPath.split('/').slice().pop().split('.json')[0]}-${iterator}.json`
		shellScript += `node index.js ${path.resolve(`${pathToWrite}/${newFileName}`)} | tee -a log.txt;\n`
		await writeFile(path.resolve(`${pathToWrite}/${newFileName}`), JSON.stringify(tempConfig, null, 2));
		
		
	}

	let finalShellScript = shellScript.trim().slice(0,-1);
	await writeFile(path.resolve(`${pathToWrite}/runAll.sh`), finalShellScript);
	console.log(`	created ${numOfConfigsNeeded} config files @ ${pathToWrite}/`)
	console.log(`\nyou can now use the generated bash script to run the entire import like this:`)
	console.log(`sh ${path.resolve(`${pathToWrite}/runAll.sh`)}`)


}


main();