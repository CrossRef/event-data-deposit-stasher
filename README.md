# Event Data Deposit Stasher

A tool for retrieving events from Lagotto and saving them in S3 for archival. Runs every day and stashes the previous day's events. Currently a proof-of-concept as of June 2016.

Slices events various ways, resulting in data in the following paths:

 - http://«host»/«YYYY-MM-DD»/all.json
 - http://«host»/«YYYY-MM-DD»/«source_type».json

Result is a single JSON object that represents all events (filtered or not) for the given day. It is in the same format that Lagotto returns from its Deposit API, except in one single page.

## Running

Create a config file in `config/dev/config.edn` or `config/prod/config.edn` or via environment variables. Keys required specified in `core.clj`.

Daily runs the whole process. Run it some time after midnight.

    lein with-profile dev run daily

A production cron job:

    0 6 * * * cd /home/deploy/event-data-deposit-stasher && lein with-profile prod run daily

This will go back up to 100 days in the past, so it could in theory be run monthly or even less frequently.

## License

Copyright © 2016 Crossref

Distributed under the MIT License.
