sh sql/initdb.sh;
python run_config.py --config config/seed_db_step_1.yaml --db_config do_db.yaml --db_map global/db_config.yaml
python run_config.py --config config/seed_db_step_2.yaml --db_config do_db.yaml --db_map global/db_config.yaml