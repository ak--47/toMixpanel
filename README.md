# to Mixpanel

## update: 8/15/23

**tldr;** this package is **deprecated**. use **[the new stuff](#new)**.

originally the design of this module was an "all-in-one" ETL to bring data out of certain vendor tools into mixpanel to allow people to self-serve on data migrations and historical backfills.

but, product analytics data is BIG, and doing any kind of significant data volume on your personal computer (over a consumer grade internet connection), with no way to "resume" a job, is unreliable.

mixpanel has evolved dramatically over the last few years and with the release of [simple identity merge](https://docs.mixpanel.com/docs/tracking/how-tos/identifying-users#simplified-vs-original-id-merge) and our [migration packages](https://docs.mixpanel.com/docs/other-bits/tutorials/migration-guides), much of the functionality in this this module is _no longer relevant_.

i have opted **not** to delete this repository, as the code works for certain cases (small data sets, mixpanel's [original identity merge](https://docs.mixpanel.com/docs/tracking/how-tos/identifying-users#simplified-vs-original-id-merge) with `$merge` events, and incremental user props), however this module will receive no further updates. 

if you are looking to migrate your data from a vendor specific format to mixpanel, these are the packages you want:

<div id="new">

### Self-Serve Data Migration Tools

**Amplitude**
- https://github.com/ak--47/amp-ext 
(extracting data)

- https://github.com/ak--47/amp-to-mp 
(transforming + loading data)

**Heap**
- https://github.com/ak--47/heap-to-mp 
(transforming + loading data)

**Adobe**
- https://github.com/ak--47/adobe-to-mp 
(transforming + loading data)

**Generic**
- https://github.com/ak--47/mixpanel-import 
(import any type of data; you write the transform)

if you need help moving your historical data to mixpanel [**contact us**](https://mixpanel.com/contact-us/sales) ... we will help! 💪
</div>

-------------

## wat.

`toMixpanel` is an ETL script in Node.js that provides one-time data migrations from common product analytics tools... to mixpanel. 

It implements Mixpanel's [`/import` ](https://developer.mixpanel.com/reference/events#import-events), [`$merge`](https://developer.mixpanel.com/reference/identities#identity-merge) and [`/engage`](https://developer.mixpanel.com/reference/user-profiles) endpoints

It uses [service accounts](https://developer.mixpanel.com/reference/authentication#service-accounts) for authentication, and can batch import millions of events and user profiles quickly.

This script is meant to be run **locally** and requires a [JSON file for configuration](https://github.com/ak--47/toMixpanel/tree/main/examples). 




## tldr;
```
git clone https://github.com/ak--47/toMixpanel.git

cd toMixpanel/

npm install

node index.js ./path-To-JSON-config
```

alternatively:

```
npx to-mixpanel ./path-To-JSON-config
```

## Detailed Instructions

### Install Dependencies

This script uses `npm` to manage dependencies, similar to a web application. 

After cloning the repo, `cd` into the `/toMixpanel` and run:

```
npm install
```

this only needs to be done once.

### Config File

`toMixpanel` requires credentials for your `source` and your `destination`

Here's an example of a configuration file for `amplitude` => `mixpanel`:

```json
{
  "source": {
    "name": "amplitude",
    "params": {
      "api_key": "{{amplitude api key}}",
      "api_secret": "{{ amplitude api secret }}",
      "start_date": "2021-09-17",
      "end_date": "2021-09-17"
    },
    "options": {
      "save_local_copy": true,
      "is EU?": false
    }
  },
  "destination": {
    "name": "mixpanel",
    "project_id": "{{ project id }}",
    "token": "{{ project token }}",
    "service_account_user": "{{ mp service account }}",
    "service_account_pass": "{{ mp service secret }}",
    "options": {
      "is EU?": false,
	  "recordsPerBatch": 2000
    }
  }
}
```

you can find more configuration examples [in the repo](https://github.com/ak--47/toMixpanel/tree/main/examples).

## supported sources
- [amplitude](https://github.com/ak--47/toMixpanel/blob/main/examples/configExample-amplitude.json)

required params: `api_key`, `api_secret`, `start_date`, `end_date`, `is EU?`


- [mixpanel](https://github.com/ak--47/toMixpanel/blob/main/examples/configExample-mixpanel.json)

that's right! you can use `toMixpanel` to migrate one mixpanel project to another!

required params: `token`, `secret`, `start_date`, `end_date`, `is EU?`, `do_events`, `do_people`

options: `where` ([see docs](https://developer.mixpanel.com/reference/segmentation-expressions)), `event` ([see docs](https://developer.mixpanel.com/reference/raw-event-export)), `recordsPerBatch` (in destination)

- [csv](https://github.com/ak--47/toMixpanel/blob/main/examples/configExample-csv.json)

required params: `filePath`, `event_name_col`, `distinct_id_col`, `time_col`, `insert_id_col`
(note: `filePath` can be EITHER a path to a CSV file or a folder which contains multiple CSV files)

- [ga360](https://github.com/ak--47/toMixpanel/blob/main/examples/configExample-ga360.json)\*

required params: `project_id`, `bucket_name`, `private_key_id`, `private_key`, `client_email`, `client_id`, `auth_uri`, `token_uri`, `auth_provider_x590_cert_url`, `client_x509_cert_url` 
options: `path_to_data` (for large datasets, does line-by-line iteration)


\*note: google analytics does not have public `/export` APIs, so you'll need to [export your data to bigQuery](https://support.google.com/analytics/answer/3437618?hl=en) *first*, and then [export your bigQuery tables to google cloud storage](https://support.google.com/analytics/answer/3416092?hl=en#zippy=,in-this-article) **as JSON**. You can then [create a service account](https://cloud.google.com/iam/docs/creating-managing-service-accounts#creating) in google cloud storage which can access the bucket; the above-mentioned values are given to you when you create a service account