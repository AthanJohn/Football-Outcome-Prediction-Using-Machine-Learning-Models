CREATE TEMPORARY TABLE home_team_home_wins AS
Select t.team_api_id, count(m.home_team_api_id) as wins
from Team t
LEFT JOIN Match m ON t.team_api_id = m.home_team_api_id AND m.home_team_goal > m.away_team_goal
group by t.team_api_id;

UPDATE home_team_home_wins SET wins = 0 WHERE wins is NULL;

CREATE TEMPORARY TABLE home_team_home_draws AS
Select t.team_api_id, count(m.home_team_api_id) as draws
from Team t
LEFT JOIN Match m ON t.team_api_id = m.home_team_api_id AND m.home_team_goal = m.away_team_goal
group by t.team_api_id;

UPDATE home_team_home_draws SET draws = 0 WHERE draws is NULL;

CREATE TEMPORARY TABLE home_team_home_losses AS
Select t.team_api_id, count(m.home_team_api_id) as losses
from Team t
LEFT JOIN Match m ON t.team_api_id = m.home_team_api_id AND m.home_team_goal < m.away_team_goal
group by t.team_api_id;

UPDATE home_team_home_losses SET losses = 0 WHERE losses is NULL;

CREATE TEMPORARY TABLE away_team_home_wins AS
Select t.team_api_id, count(m.away_team_api_id) as wins
from Team t
LEFT JOIN Match m ON t.team_api_id = m.away_team_api_id AND m.home_team_goal < m.away_team_goal
group by t.team_api_id;

UPDATE away_team_home_wins SET wins = 0 WHERE wins is NULL;

CREATE TEMPORARY TABLE away_team_home_draws AS
Select t.team_api_id, count(m.away_team_api_id) as draws
from Team t
LEFT JOIN Match m ON t.team_api_id = m.away_team_api_id AND m.home_team_goal + m.away_team_goal
group by t.team_api_id;

UPDATE away_team_home_draws SET draws = 0 WHERE draws is NULL;

CREATE TEMPORARY TABLE away_team_home_losses AS
Select t.team_api_id, count(m.away_team_api_id) as losses
from Team t
LEFT JOIN Match m ON t.team_api_id = m.away_team_api_id AND m.home_team_goal > m.away_team_goal
group by t.team_api_id;

UPDATE away_team_home_losses SET losses = 0 WHERE losses is NULL;

INSERT INTO Team_Wins_Draws_Losses
SELECT  t.team_api_id,
		t.team_long_name,
		t.team_short_name,
		hthw.wins + athw.wins as wins,
		hthd.draws + athd.draws as draws,
		hthl.losses + athl.losses as losses
FROM Team t
LEFT JOIN home_team_home_wins hthw ON t.team_api_id = hthw.team_api_id
LEFT JOIN home_team_home_draws hthd ON t.team_api_id = hthd.team_api_id
LEFT JOIN home_team_home_losses hthl ON t.team_api_id = hthl.team_api_id
LEFT JOIN away_team_home_wins athw ON t.team_api_id = athw.team_api_id
LEFT JOIN away_team_home_draws athd ON t.team_api_id = athd.team_api_id
LEFT JOIN away_team_home_losses athl ON t.team_api_id = athl.team_api_id;
 
DROP TABLE home_team_home_wins;
DROP TABLE home_team_home_draws;
DROP TABLE home_team_home_losses;
DROP TABLE away_team_home_wins;
DROP TABLE away_team_home_draws;
DROP TABLE away_team_home_losses;
--DELETE FROM Team_Wins_Draws_Losses
--select * from team 
--where team_api_id not in (select team_api_id from Team_Wins_Draws_Losses)
--9746
--8357


SELECT * FROM Team_Wins_Draws_Losses
--8357
SELECT * FROM Match
WHERE away_team_api_id = 8357 and away_team_goal > home_team_goal

select * from Team_Wins_Draws_Losses
where team_api_id = 8357

select * from Match
where home_player_X1 is null and home_player_1 is not null and goal is not null
