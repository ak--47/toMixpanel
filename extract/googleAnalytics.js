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
    console.log(`   auth with bucket: ${bucketName} as ${keyFileData.client_email.split('@')[0]}`)
    //list files
    const [files] = await (await storage.bucket(bucketName).getFiles());

    //collect files paths for further processing
    const listOfDownloadedFiles = [];
    const listOfgzipFiles = [];

    //consume bucket
    const fileNames = files.map(f => f.name);
    for (const fileName of fileNames) {
        const pathToFile = path.resolve(`${directory}/downloaded/${fileName}`);
        const options = {
            destination: pathToFile
        };
        await storage.bucket(bucketName).file(fileName).download(options);
        console.log(`   downloaded ${fileName}`);

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

    //ungzip files
    const listOfgunzipFiles = []
    for (const filePath of listOfgzipFiles) {
        let dataFile = await readFilePromisified(filePath);
        let gunzipped = await gun.ungzip(dataFile)
        let gaRawData = gunzipped.toString('utf-8');
        let pathToWriteFile = path.resolve(`${directory}/gunzip/${filePath.split('/').pop()}`)
        await writeFilePromisified(pathToWriteFile, gaRawData);
        listOfgunzipFiles.push(pathToWriteFile);
    }

    const fullListOfFilePaths = [...listOfDownloadedFiles, ...listOfgunzipFiles];
    const validFilePaths = [];

    //TODO verify files as JSON or NSJSON


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