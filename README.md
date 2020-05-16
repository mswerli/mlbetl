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
- Builds are defined by using a yaml config file. See `config/seed_db.yaml` and `config/2018.yaml` for examples. Each config defines the types for data a build includes and the steps to run on those data
- Request are created and made using methods contained in `utility_classes/url_factory.py`. Resulting data are stored in the output directories specified in a build config
- Files are read in using methods contained in `utility_classes/file_reader.py` and loaded using methods from `utility_classes/postgres_table.py`
- These utility classes are used to build super classes for players (`plyer.py`), rosters/teams (`roster.py`), and at bat data (`atBat.py`)
- Build configs are executed using `run_config.py` (excpet for the predefined `seed_db.yaml` config which can be run using `seed_db.py`)

Database
- Connection information should be defined in a yaml file similar to `config/local_db.yaml`.
- There is a dockerfile and a docker compose file which should allow (by running `docker-compose up`) for a local postgres database to be spun up and for neccesary tables to be created (see `sql/initdb.sh`)
- If you care to run a postgres instance outside of the docker setup provided, just generate a cofiguration yaml for the db

Ideas for analysis
- Within the `sql` directory there are various sql scripts I've put together that I think are interesting

### Potential Future Work
- Bringing in aggregated hitting and pitching stats from the MLB Data API. This would augment at bat data which doesn't have information on certain thing (IBBs and earned runs being examples)
- Impoved usage of psycopg2's copy options. Currently uploading a seasons worth of at bat data takes about an hour
- Allow for using s3 or other cloudbased storage options rather than local file system

Along with  this project, I'm working on building a [tool using R Shiny)\](https://github.com/mswerli/rstat_class) to visualize this.

If anyone comes across this and are interested in new features or contributing, please email me at mswerli@gmail.com.