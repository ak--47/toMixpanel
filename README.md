# to Mixpanel

## wat.

`toMixpanel` is an ETL script in Node.js that provides one-time data migrations from common product analytics tools... to mixpanel. 

It implements Mixpanel's [`/import` ](https://developer.mixpanel.com/reference/events#import-events), [`$merge`](https://developer.mixpanel.com/reference/identities#identity-merge) and [`/engage`](https://developer.mixpanel.com/reference/user-profiles) endpoints

It uses [service accounts](https://developer.mixpanel.com/reference/authentication#service-accounts) for authentication, and can batch import millions of events and user profiles quickly.

This script is meant to be run **locally** and requires a [JSON file for configuration](https://github.com/ak--47/toMixpanel/blob/main/configExample.json). 




## tldr;
```
git clone https://github.com/ak--47/toMixpanel.git

cd toMixpanel/

npm install

node index.js ./path-To-JSON-config
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

Here's an example of a configuration file:

```
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
      "is EU?": false
    }
  }
}
```
## supported sources
- amplitude
required params: `api_key`, `api_secret`, `start_date`, `end_date`, `is EU?`

