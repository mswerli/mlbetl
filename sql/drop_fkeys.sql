ALTER TABLE rosters.historical_rosters DROP CONSTRAINT historical_rosters_player_id_fkey;
ALTER TABLE rosters.historical_rosters DROP CONSTRAINT historical_rosters_team_id_fkey;
ALTER TABLE rosters.roster_40 DROP CONSTRAINT roster_40_player_id_fkey;
ALTER TABLE rosters.roster_40 DROP CONSTRAINT roster_40_team_id_fkey;
ALTER TABLE rosters.transactions DROP CONSTRAINT transactions_from_team_id_fkey;
ALTER TABLE rosters.transactions DROP CONSTRAINT transactions_player_id_fkey;
ALTER TABLE rosters.transactions DROP CONSTRAINT transactions_team_id_fkey;