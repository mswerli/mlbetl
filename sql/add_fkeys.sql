--ALTER TABLE rosters.historical_rosters
--   ADD CONSTRAINT historical_rosters_player_id_fkey FOREIGN KEY (player_id)
--    REFERENCES rosters.players (player_id) MATCH SIMPLE
--   ON UPDATE NO ACTION
--    ON DELETE NO ACTION;

ALTER TABLE rosters.historical_rosters
    ADD CONSTRAINT historical_rosters_team_id_fkey FOREIGN KEY (team_id)
    REFERENCES league.teams (team_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

-- ALTER TABLE rosters.roster_40
--    ADD CONSTRAINT roster_40_player_id_fkey FOREIGN KEY (player_id)
--   REFERENCES rosters.players (player_id) MATCH SIMPLE
--    ON UPDATE NO ACTION
--   ON DELETE NO ACTION;

ALTER TABLE rosters.roster_40
    ADD CONSTRAINT roster_40_team_id_fkey FOREIGN KEY (team_id)
    REFERENCES league.teams (team_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE rosters.transactions
    ADD CONSTRAINT transactions_from_team_id_fkey FOREIGN KEY (from_team_id)
    REFERENCES league.teams (team_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

-- ALTER TABLE rosters.transactions
--   ADD CONSTRAINT transactions_player_id_fkey FOREIGN KEY (player_id)
--    REFERENCES rosters.players (player_id) MATCH SIMPLE
--   ON UPDATE NO ACTION
--    ON DELETE NO ACTION;

ALTER TABLE rosters.transactions
    ADD CONSTRAINT transactions_team_id_fkey FOREIGN KEY (team_id)
    REFERENCES league.teams (team_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;
