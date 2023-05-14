--Team's home total goals scored
INSERT INTO Team_Total_Home_Goals
SELECT	T.team_api_id,
		T.team_long_name,
		SUM(MH.home_team_goal)
FROM Team T
INNER JOIN Match MH ON T.team_api_id = MH.home_team_api_id
GROUP BY T.team_api_id, T.team_long_name

--Team's away total goals scored
INSERT INTO Team_Total_Away_Goals
SELECT	T.team_api_id,
		T.team_long_name,
		SUM(MA.home_team_goal)
FROM Team T
INNER JOIN Match MA ON T.team_api_id = MA.away_team_api_id
GROUP BY T.team_api_id, T.team_long_name

--Team's home total goals conceded
INSERT INTO Team_Total_Home_Goals_C
SELECT	T.team_api_id,
		T.team_long_name,
		SUM(MH.home_team_goal)
FROM Team T
INNER JOIN Match MH ON T.team_api_id = MH.away_team_api_id
GROUP BY T.team_api_id, T.team_long_name

--Team's away total goals conceded
INSERT INTO Team_Total_Away_Goals_C
SELECT	T.team_api_id,
		T.team_long_name,
		SUM(MA.home_team_goal)
FROM Team T
INNER JOIN Match MA ON T.team_api_id = MA.home_team_api_id
GROUP BY T.team_api_id, T.team_long_name

INSERT INTO Team_Total_Goals
SELECT	T.team_api_id,
		T.team_long_name,
		T,team_short_name,
		H.team_goals,
		A.team_goals,
		H.team_goals + A.team_goals AS teamGoalsScored,
		HC.team_goals,
		AC.team_goals,
		HC.team_goals + AC.team_goals AS teamGoalsConceded,
		teamGoalsScored - teamGoalsConceded
FROM Team T
INNER JOIN Team_Total_Away_Goals A ON T.team_api_id = A.team_api_id
INNER JOIN Team_Total_Home_Goals H ON T.team_api_id = H.team_api_id
INNER JOIN Team_Total_Away_Goals_C AC ON T.team_api_id = AC.team_api_id
INNER JOIN Team_Total_Home_Goals_C HC ON T.team_api_id = HC.team_api_id

select * from Team_Total_Goals