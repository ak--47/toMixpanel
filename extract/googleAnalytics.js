import { Storage } from '@google-cloud/storage';
import * as fs from 'fs';
import * as path from 'path';
import { mkdirSync, existsSync } from 'fs';
import { default as gun } from 'node-gzip';
import { promisify } from 'util'


const readFilePromisified = promisify(fs.readFile);
const writeFilePromisified = promisify(fs.writeFile);


export default async function main(projectId, bucketName, keyFileData, destPath) {
    const directory = path.resolve(`./savedData/${destPath}/`)
    const keyFilename = path.resolve(`${directory}/credentials.json`);
    const credFile = fs.writeFileSync(keyFilename, JSON.stringify(keyFileData));

    //make some sub directories
    mkdirSync(`${directory}/downloaded`)
    mkdirSync(`${directory}/json`)

    //auth
    const storage = new Storage({ projectId, keyFilename });
    console.log(`   attempting to auth with bucket: ${bucketName} as ${keyFileData.client_email.split('@')[0]}`)

    try {
        await (await storage.bucket(bucketName).getFiles());
        console.log(`   successfully authenticated!\n`)
    } catch (e) {
        console.log(`   failed to auth with cloudStorage (check your credentials)\n\n`)
    }

    //list files
    const [files] = await (await storage.bucket(bucketName).getFiles());
    //collect files paths for further processing
    const listOfDownloadedFiles = [];
    const listOfgzipFiles = [];

    //consume bucket
    console.log(`   downloading files`)
    const fileNames = files.map(f => f.name);
    downloadFiles: for (const fileName of fileNames) {
        const pathToFile = path.resolve(`${directory}/downloaded/${fileName}`);
        const options = {
            destination: pathToFile
        };
        await storage.bucket(bucketName).file(fileName).download(options);
        const size = calcSize(pathToFile)
        console.log(`      downloaded ${fileName} (${size} MB)`);

        //gzip test;
        let rawFile = await readFilePromisified(pathToFile);
        if (isGzip(rawFile)) {
            listOfgzipFiles.push(pathToFile)
            if (!existsSync(`${directory}/gunzip`)) {
                mkdirSync(`${directory}/gunzip`)
            }
        } else {
            listOfDownloadedFiles.push(pathToFile);
        }

    }

    console.log(`\n   consumed entire bucket: ${bucketName} (${fileNames.length} files)\n`)

    if (listOfgzipFiles.length > 0) {
        console.log(`   gunzipping files...\n`)
    }
    //ungzip files
    const listOfgunzipFiles = []
    gunzipFiles: for (const filePath of listOfgzipFiles) {
        let dataFile = await readFilePromisified(filePath);
        let gunzipped = await gun.ungzip(dataFile);
        let gaRawData = gunzipped.toString('utf-8');
        let fileName = filePath.split('/').pop();
        let pathToWriteFile = path.resolve(`${directory}/gunzip/${fileName}`)
        await writeFilePromisified(pathToWriteFile, gaRawData);
        listOfgunzipFiles.push(pathToWriteFile);
        const size = calcSize(pathToWriteFile);
        console.log(`       gunzipped ${fileName} (${size} MB)`);
    }

    const fullListOfFilePaths = [...listOfDownloadedFiles, ...listOfgunzipFiles];
    const validFilePaths = [];
    console.log(`\n   verifying ${fullListOfFilePaths.length} downloaded files`)
    //verify files as JSON or NSJSON
    verifyFiles: for (const pathToFile of fullListOfFilePaths) {
        const fileData = (await readFilePromisified(pathToFile, "utf8")).trim();
        const fileName = pathToFile.split('/').pop();
        const writePath = path.resolve(`${directory}/json/${fileName}${fileName.endsWith(".json") ? "": ".json"}`);
        let parsed;
        try {
            parsed = JSON.parse(fileData);
            await writeFilePromisified(writePath, JSON.stringify(parsed));
            console.log(`       sucessfully parsed ${fileName} as JSON`)
            validFilePaths.push(writePath)
        } catch (e) {
            //it's probably NDJSON, so iterate over each line
            try {
                parsed = fileData.split('\n').map(line => JSON.parse(line));
                await writeFilePromisified(writePath, JSON.stringify(parsed));
                console.log(`   sucessfully parsed ${fileName} as NDJSON`)
                validFilePaths.push(writePath)
            } catch (e) {
                //if we don't have JSON or NDJSON... skip...
                console.log(`   failed to parse: ${fileName} (not JSON/NDJSON) skipping...`)
                continue verifyFiles;
            }
        }
    }


    return validFilePaths;

}

function smartCommas(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function isGzip(buf) {
    if (!buf || buf.length < 3) {
        return false;
    }
    return buf[0] === 0x1F && buf[1] === 0x8B && buf[2] === 0x08;
};

function calcSize(file) {
    const size = smartCommas(((fs.statSync(file).size) / (1024 * 1024)).toFixed(2));
    return size;
}