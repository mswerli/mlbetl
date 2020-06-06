### MLBETL
Replication framework for extracting data from various baseball APIs and loading data into a postgres database

#### Sources
MLB Data API
- Used for players, historical rosters, Current 40 man rosters, and teams
- https://appac.github.io/mlb-data-api-docs/

Baseball Savant
- Used for at bat data, including pitch info and batted ball info (stat cast)
- https://baseballsavant.mlb.com/statcast_search

#### How it works
- Builds are defined by using a yaml config file. See `config/teams_and_rosters.yaml` and `config/player_atbat_schedule.yaml` for examples.
- The class defined in `classes/config_class.py` ingests a config file and generates the urls to send requests. This object gets passed throughout the build sequence
- The extract, transform, and load classes run the corresponding steps for any endpoint defined in the config object
- Some details on the libraries used for each step:
** Extract - Uses requests to hit http endpoints defined in `config/api.yaml`
** Transform - Uses pandas (for data type conversion and column selection) and jsonpath-ng for getting needed data out of json files. Output of this step is a csv
** Load - Uses psycopg2, specifically the `copy_from` method to copy csvs from the transform step to a postgresql database

Database
- Connection information should be defined in a yaml file similar to `config/local_db.yaml`.
- There is a dockerfile and a docker compose file which should allow (by running `docker-compose up`) for a local postgres database to be spun up and for neccesary tables to be created (see `sql/initdb.sh`)
- If you care to run a postgres instance outside of the docker setup provided, just generate a cofiguration yaml for the db

Things to consider:
- Currently extracting data from the baseball savant endpoint is time consuming. Requests have to be spaced out which results in increased wait times
- At bat data will typically consist of ~750,000 rows per season if all teams are included. With this is mind, when running locally limit the at bat data to a single season or a subset of teams
- Certain tables/types of data depend on each other for builds
** Unless specific player ids are provided, the extract step for players wil look at the `rosters.missing_players` mv for which IDs are needed

Ideas for analysis
- Within the `sql` directory there are various sql scripts I've put together that I think are interesting

Along with  this project, I'm working on building a [tool using R Shiny)\](https://github.com/mswerli/rstat_class) to visualize resulting data.

If anyone comes across this and is interested in new features or contributing, please email me at mswerli@gmail.com.