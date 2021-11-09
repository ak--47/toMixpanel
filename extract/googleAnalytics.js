import { Storage } from '@google-cloud/storage';
import * as fs from 'fs';
import * as path from 'path';
import { mkdirSync } from 'fs';



export default async function main(projectId, bucketName, keyFileData, destPath) {
    const directory = path.resolve(`./savedData/${destPath}/`)
    const keyFilename = path.resolve(`${directory}/credentials.json`);
    const credFile = fs.writeFileSync(keyFilename, JSON.stringify(keyFileData));

    //make some sub directories
    mkdirSync(`${directory}/downloaded`)
    mkdirSync(`${directory}/gunzip`)
    mkdirSync(`${directory}/json`)

    //auth
    const storage = new Storage({ projectId, keyFilename });

    //list files
    const [files] = await (await storage.bucket(bucketName).getFiles());
    const fileNames = files.map(f => f.name);

    //consume bucket
    for (const fileName of fileNames) {
        const options = {
            destination: path.resolve(`${directory}/downloaded/${fileName}`),
        };
        await storage.bucket(bucketName).file(fileName).download(options);

        console.log(`gs://${bucketName}/${fileName} downloaded to ${fileName}.`);
    }
}