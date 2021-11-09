import gaExtract from '../extract/googleAnalytics.js'
import gaTransform from '../transform/gaToMixpanel.js'

async function googleAnalyticsETL(config, directoryName) {
    const { bucket_name, keyFile, project_id } = config.source.params;

    console.log('EXTRACT!\n')
    let extractedFiles = await gaExtract(project_id, bucket_name, keyFile, directoryName);

    console.log('\nTRANSFORM!\n')
    let transform = await gaTransform(extractedFiles, `./savedData/${directoryName}`, config.destination.token);


    console.log('\nLOAD!\n')


}


export default googleAnalyticsETL;