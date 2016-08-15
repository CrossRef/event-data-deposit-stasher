# Event Data Deposit Stasher

A tool for mainaining a public Event Data API via S3. Comprises daily tasks that update the archive and the Query API buckets. The result is a static API that answers the following queries:

Archive:

 - http://«archive-api-host»/collected/«YYYY-MM-DD»/deposits.json

Query API:

 - `http://«query-api-host»/collected/«YYYY-MM-DD»/events.json` - all events collected on given day.
 - `http://«query-api-host»/collected/«YYYY-MM-DD»/sources/«source_type»/events.json` - events for given source type collected on given day.
 - `http://«query-api-host»/collected/«YYYY-MM-DD»/works/«DOI»/events.json` - events for given DOI collected on given day.
 - `http://«query-api-host»/collected/«YYYY-MM-DD»/works/«DOI»/sources/«source_type»/events.json` - events for given DOI of given source type collected on given day.

All Query API data can be regenerated from the corresponding deposits archive. Content of Deposits is currently what Lagotto produces. Content of Events is the current iteration of the CED Events Schema document.

## Status

Currently a **prototype proof-of-concept** as of August 2016.

As of August 2016, Crossref has approximatley 40,000 events per day. This means the base `events.json` is 20MB.

Currently it takes approximately a minute or two to run a day's worth of updating. Requires about 2GB RAM because it does a lot of work in parallel. 

## Running

Create a config file in `config/dev/config.edn` or `config/prod/config.edn` or via environment variables. Keys required specified in `core.clj`.

Daily runs the whole process. Run it some time after midnight.

`daily-archive` fetches all deposits from Lagotto and archives.

    lein with-profile dev run daily-archive

`daily-load-query-api` fetches deposits from archive and updates Query API.

    lein with-profile dev run daily-load-query-api

A production cron job:

    0 6 * * * cd /home/deploy/event-data-deposit-stasher && lein with-profile prod run daily-archive && lein with-profile dev run daily-load-query-api

## Back-filling

The daily task only processes (or reprocesses) the last day. Run the following to reprocess/refetch the last 100 days.

    lein with-profile dev run historical-archive
    lein with-profile dev run historical-load-query-api

Data is not re-generated if it already exists in the target bucket. If you want to re-process archive, remove:

    «archive-bucket»/collected/«YYYY-MM-DD»/deposits.json

If you want to reprocess Query API for a given day, remove:

    «query-api-bucket»/collected/«YYYY-MM-DD»/events.json

All Query endpoints for a given day (up to 4,000) are re-processed if the base `events.json` is removed.

## License

Copyright © 2016 Crossref

Distributed under the MIT License.
