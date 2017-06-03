BEGIN TRANSACTION;
CREATE TEMPORARY TABLE t1_backup(day, month, year, player_id, game_id, make, quarter, team, minute, second, id, season
);
INSERT INTO t1_backup SELECT day, month, year, player_id, game_id, make, quarter, team, minute, second, id, season
 FROM dunks;
DROP TABLE dunks;
CREATE TABLE dunks(day, month, year, player_id, game_id, make, quarter, team, minute, second, id, season
);
INSERT INTO dunks SELECT day, month, year, player_id, game_id, make, quarter, team, minute, second, id, season
 FROM t1_backup;
DROP TABLE t1_backup;
COMMIT;
