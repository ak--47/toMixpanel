import gaExtract from '../extract/googleAnalytics.js'

async function googleAnalyticsETL(config, directoryName) {
    const { bucket_name, keyFile, project_id } = config.source.params;

    console.log('EXTRACT!\n')
    let extract = await gaExtract(project_id, bucket_name, keyFile, directoryName);

    console.log('TRANSFORM!\n')


    console.log('LOAD!')


}


export default googleAnalyticsETL;