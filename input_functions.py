import pandas as pd
from bs4 import BeautifulSoup
import warnings
from sklearn.preprocessing import MinMaxScaler


def GetInputTable(cursor, conn):
    '''Get data using SQLite commands from the SQLite database.'''

    cursor.execute('''
    create temp table home_team_attributes as
    select 
    	m.match_api_id, 
    	m.date as match_date, 
    	ht.date as home_team_date, 
    	m.home_team_api_id, 
    	ht.buildUpPlaySpeed, 
    	ht.buildUpPlayDribbling, 
    	ht.buildUpPlayPassing,
    	ht.buildUpPlayPositioningClass,
    	ht.chanceCreationPassing,
    	ht.chanceCreationCrossing,
    	ht.chanceCreationShooting,
    	ht.chanceCreationPositioningClass,
    	ht.defencePressure,
    	ht.defenceAggression,
    	ht.defenceTeamWidth,
    	ht.defenceDefenderLineClass
    from Match m
    inner join ( SELECT m.home_team_api_id,
    			  m.date,
    			  MIN(ABS(JULIANDAY(ht.date) - JULIANDAY(m.date))) as dd
    			  from match m
    			  left join Team_Attributes ht
    			  on m.home_team_api_id = ht.team_api_id
    			  group by m.home_team_api_id, m.date) d
    ON d.home_team_api_id = m.home_team_api_id and d.date = m.date
    left join Team_Attributes ht
    on ht.team_api_id = m.home_team_api_id and abs(JULIANDAY(ht.date) - JULIANDAY(m.date)) = d.dd
    order by m.date desc;
                   ''')

    cursor.execute('''
    create temp table away_team_attributes as
    select 
    	m.match_api_id, 
    	m.date as match_date, 
    	at.date as away_team_date, 
    	m.away_team_api_id, 
    	at.buildUpPlaySpeed, 
    	at.buildUpPlayDribbling, 
    	at.buildUpPlayPassing,
    	at.buildUpPlayPositioningClass,
    	at.chanceCreationPassing,
    	at.chanceCreationCrossing,
    	at.chanceCreationShooting,
    	at.chanceCreationPositioningClass,
    	at.defencePressure,
    	at.defenceAggression,
    	at.defenceTeamWidth,
    	at.defenceDefenderLineClass
    from Match m
    inner join ( SELECT m.away_team_api_id,
    			  m.date,
    			  MIN(ABS(JULIANDAY(at.date) - JULIANDAY(m.date))) as dd
    			  from match m
    			  left join Team_Attributes at
    			  on m.away_team_api_id = at.team_api_id
    			  group by m.away_team_api_id, m.date) d
    ON d.away_team_api_id = m.away_team_api_id and d.date = m.date
    left join Team_Attributes at
    on at.team_api_id = m.away_team_api_id and abs(JULIANDAY(at.date) - JULIANDAY(m.date)) = d.dd
    order by m.date desc
                   ''')

    query = '''
    SELECT 
    	M.match_api_id,
        M.Date,
    	M.home_team_api_id,
        M.away_team_api_id,
    	HTA.buildUpPlaySpeed AS home_buildUpPlaySpeed, 
    	HTA.buildUpPlayDribbling AS home_buildUpPlayDribbling, 
    	HTA.buildUpPlayPassing AS home_buildUpPlayPassing,
    	HTA.buildUpPlayPositioningClass AS home_buildUpPlayPositioning,
    	HTA.chanceCreationPassing AS home_chanceCreationPassing,
    	HTA.chanceCreationCrossing AS home_chanceCreationCrossing,
    	HTA.chanceCreationShooting AS home_chanceCreationShooting,
    	HTA.chanceCreationPositioningClass AS home_chanceCreationPositioning,
    	HTA.defencePressure AS home_defencePressure,
    	HTA.defenceAggression AS home_defenceAggression,
    	HTA.defenceTeamWidth AS home_defenceTeamWidth,
    	HTA.defenceDefenderLineClass AS home_defenceDefenderLine,
    	ATA.buildUpPlaySpeed AS away_buildUpPlaySpeed, 
    	ATA.buildUpPlayDribbling AS away_buildUpPlayDribbling, 
    	ATA.buildUpPlayPassing AS away_buildUpPlayPassing,
    	ATA.buildUpPlayPositioningClass AS away_buildUpPlayPositioning,
    	ATA.chanceCreationPassing AS away_chanceCreationPassing,
    	ATA.chanceCreationCrossing AS away_chanceCreationCrossing,
    	ATA.chanceCreationShooting AS away_chanceCreationShooting,
    	ATA.chanceCreationPositioningClass AS away_chanceCreationPositioning,
    	ATA.defencePressure AS away_defencePressure,
    	ATA.defenceAggression AS away_defenceAggression,
    	ATA.defenceTeamWidth AS away_defenceTeamWidth,
    	ATA.defenceDefenderLineClass AS away_defenceDefenderLine,
    	M.home_team_goal, 
    	M.away_team_goal,
    	M.home_player_X1,
    	M.home_player_X2,
    	M.home_player_X3,
    	M.home_player_X4,
    	M.home_player_X5,
    	M.home_player_X6,
    	M.home_player_X7,
    	M.home_player_X8,
    	M.home_player_X9,
    	M.home_player_X10,
    	M.home_player_X11,
    	M.home_player_Y1,
    	M.home_player_Y2,
    	M.home_player_Y3,
    	M.home_player_Y4,
    	M.home_player_Y5,
    	M.home_player_Y6,
    	M.home_player_Y7,
    	M.home_player_Y8,
    	M.home_player_Y9,
    	M.home_player_Y10,
    	M.home_player_Y11,
    	M.away_player_X1,
    	M.away_player_X2,
    	M.away_player_X3,
    	M.away_player_X4,
    	M.away_player_X5,
    	M.away_player_X6,
    	M.away_player_X7,
    	M.away_player_X8,
    	M.away_player_X9,
    	M.away_player_X10,
    	M.away_player_X11,
    	M.away_player_Y1,
    	M.away_player_Y2,
    	M.away_player_Y3,
    	M.away_player_Y4,
    	M.away_player_Y5,
    	M.away_player_Y6,
    	M.away_player_Y7,
    	M.away_player_Y8,
    	M.away_player_Y9,
    	M.away_player_Y10,
    	M.away_player_Y11,
    	M.home_player_1,
    	ROUND((julianday(M.date) - julianday(HP1.birthday)) / 365.25, 2) AS home_player_1_age,
    	HP1.height AS home_player_1_height,
    	HP1.weight AS home_player_1_weight,
    	M.home_player_2,
    	ROUND((julianday(M.date) - julianday(HP2.birthday)) / 365.25, 2) AS home_player_2_age,
    	HP2.height AS home_player_2_height,
    	HP2.weight AS home_player_2_weight,
    	M.home_player_3,
    	ROUND((julianday(M.date) - julianday(HP3.birthday)) / 365.25, 2) AS home_player_3_age,
    	HP3.height AS home_player_3_height,
    	HP3.weight AS home_player_3_weight,
    	M.home_player_4,
    	ROUND((julianday(M.date) - julianday(HP4.birthday)) / 365.25, 2) AS home_player_4_age,
    	HP4.height AS home_player_4_height,
    	HP4.weight AS home_player_4_weight,
    	M.home_player_5,
    	ROUND((julianday(M.date) - julianday(HP5.birthday)) / 365.25, 2) AS home_player_5_age,
    	HP5.height AS home_player_5_height,
    	HP5.weight AS home_player_5_weight,
    	M.home_player_6,
    	ROUND((julianday(M.date) - julianday(HP6.birthday)) / 365.25, 2) AS home_player_6_age,
    	HP6.height AS home_player_6_height,
    	HP6.weight AS home_player_6_weight,
    	M.home_player_7,
    	ROUND((julianday(M.date) - julianday(HP7.birthday)) / 365.25, 2) AS home_player_7_age,
    	HP7.height AS home_player_7_height,
    	HP7.weight AS home_player_7_weight,
    	M.home_player_8,
    	ROUND((julianday(M.date) - julianday(HP8.birthday)) / 365.25, 2) AS home_player_8_age,
    	HP8.height AS home_player_8_height,
    	HP8.weight AS home_player_8_weight,
    	M.home_player_9,
    	ROUND((julianday(M.date) - julianday(HP9.birthday)) / 365.25, 2) AS home_player_9_age,
    	HP9.height AS home_player_9_height,
    	HP9.weight AS home_player_9_weight,
    	M.home_player_10,
    	ROUND((julianday(M.date) - julianday(HP10.birthday)) / 365.25, 2) AS home_player_10_age,
    	HP10.height AS home_player_10_height,
    	HP10.weight AS home_player_10_weight,
    	M.home_player_11,
    	ROUND((julianday(M.date) - julianday(HP11.birthday)) / 365.25, 2) AS home_player_11_age,
    	HP11.height AS home_player_11_height,
    	HP11.weight AS home_player_11_weight,
    	M.away_player_1,
    	ROUND((julianday(M.date) - julianday(AP1.birthday)) / 365.25, 2) AS away_player_1_age,
    	AP1.height AS away_player_1_height,
    	AP1.weight AS away_player_1_weight,
    	M.away_player_2,
    	ROUND((julianday(M.date) - julianday(AP2.birthday)) / 365.25, 2) AS away_player_2_age,
    	AP2.height AS away_player_2_height,
    	AP2.weight AS away_player_2_weight,
    	M.away_player_3,
    	ROUND((julianday(M.date) - julianday(AP3.birthday)) / 365.25, 2) AS away_player_3_age,
    	AP3.height AS away_player_3_height,
    	AP3.weight AS away_player_3_weight,
    	M.away_player_4,
    	ROUND((julianday(M.date) - julianday(AP4.birthday)) / 365.25, 2) AS away_player_4_age,
    	AP4.height AS away_player_4_height,
    	AP4.weight AS away_player_4_weight,
    	M.away_player_5,
    	ROUND((julianday(M.date) - julianday(AP5.birthday)) / 365.25, 2) AS away_player_5_age,
    	AP5.height AS away_player_5_height,
    	AP5.weight AS away_player_5_weight,
    	M.away_player_6,
    	ROUND((julianday(M.date) - julianday(AP6.birthday)) / 365.25, 2) AS away_player_6_age,
    	AP6.height AS away_player_6_height,
    	AP6.weight AS away_player_6_weight,
    	M.away_player_7,
    	ROUND((julianday(M.date) - julianday(AP7.birthday)) / 365.25, 2) AS away_player_7_age,
    	AP7.height AS away_player_7_height,
    	AP7.weight AS away_player_7_weight,
    	M.away_player_8,
    	ROUND((julianday(M.date) - julianday(AP8.birthday)) / 365.25, 2) AS away_player_8_age,
    	AP8.height AS away_player_8_height,
    	AP8.weight AS away_player_8_weight,
    	M.away_player_9,
    	ROUND((julianday(M.date) - julianday(AP9.birthday)) / 365.25, 2) AS away_player_9_age,
    	AP9.height AS away_player_9_height,
    	AP9.weight AS away_player_9_weight,
    	M.away_player_10,
    	ROUND((julianday(M.date) - julianday(AP10.birthday)) / 365.25, 2) AS away_player_10_age,
    	AP10.height AS away_player_10_height,
    	AP10.weight AS away_player_10_weight,
    	M.away_player_11,
    	ROUND((julianday(M.date) - julianday(AP11.birthday)) / 365.25, 2) AS away_player_11_age,
    	AP11.height AS away_player_11_height,
    	AP11.weight AS away_player_11_weight,
        HTG.teamGoalsScored AS home_team_total_goals_scored,
        ATG.teamGoalsScored AS away_team_total_goals_scored,
        HTG.teamGoalsConceded AS home_team_total_goals_conceded,
        ATG.teamGoalsConceded AS away_team_total_goals_conceded,
        HTG.teamGoalDifference AS home_team_total_goals_diff,
        ATG.teamGoalDifference AS away_team_total_goals_diff,
        HTWDL.team_wins AS home_team_total_wins,
        HTWDL.team_draws AS home_team_total_draws,
        HTWDL.team_losses AS home_team_total_losses,
        ATWDL.team_wins AS away_team_total_wins,
        ATWDL.team_draws AS away_team_total_draws,
        ATWDL.team_losses AS away_team_total_losses,
    	M.goal,
    	M.shoton,
    	M.shotoff,
    	M.foulcommit,
    	M.card,
    	M.cross,
    	M.corner,
    	M.possession,
    	CASE
    		WHEN M.home_team_goal > M.away_team_goal THEN 1
    		WHEN M.home_team_goal < M.away_team_goal THEN 2
    		ELSE 0
    	END AS result
    FROM Match AS M
    LEFT JOIN Player HP1 ON M.home_player_1 = HP1.player_api_id
    LEFT JOIN Player HP2 ON M.home_player_2 = HP2.player_api_id
    LEFT JOIN Player HP3 ON M.home_player_3 = HP3.player_api_id
    LEFT JOIN Player HP4 ON M.home_player_4 = HP4.player_api_id
    LEFT JOIN Player HP5 ON M.home_player_5 = HP5.player_api_id
    LEFT JOIN Player HP6 ON M.home_player_6 = HP6.player_api_id
    LEFT JOIN Player HP7 ON M.home_player_7 = HP7.player_api_id
    LEFT JOIN Player HP8 ON M.home_player_8 = HP8.player_api_id
    LEFT JOIN Player HP9 ON M.home_player_9 = HP9.player_api_id
    LEFT JOIN Player HP10 ON M.home_player_10 = HP10.player_api_id
    LEFT JOIN Player HP11 ON M.home_player_11 = HP11.player_api_id
    LEFT JOIN Player AP1 ON M.away_player_1 = AP1.player_api_id
    LEFT JOIN Player AP2 ON M.away_player_2 = AP2.player_api_id
    LEFT JOIN Player AP3 ON M.away_player_3 = AP3.player_api_id
    LEFT JOIN Player AP4 ON M.away_player_4 = AP4.player_api_id
    LEFT JOIN Player AP5 ON M.away_player_5 = AP5.player_api_id
    LEFT JOIN Player AP6 ON M.away_player_6 = AP6.player_api_id
    LEFT JOIN Player AP7 ON M.away_player_7 = AP7.player_api_id
    LEFT JOIN Player AP8 ON M.away_player_8 = AP8.player_api_id
    LEFT JOIN Player AP9 ON M.away_player_9 = AP9.player_api_id
    LEFT JOIN Player AP10 ON M.away_player_10 = AP10.player_api_id
    LEFT JOIN Player AP11 ON M.away_player_11 = AP11.player_api_id
    LEFT JOIN home_team_attributes HTA ON M.match_api_id = HTA.match_api_id
    LEFT JOIN away_team_attributes ATA ON M.match_api_id = ATA.match_api_id
    LEFT JOIN Team_Total_Goals HTG ON M.home_team_api_id = HTG.team_api_id
    LEFT JOIN Team_Total_Goals ATG ON M.away_team_api_id = ATG.team_api_id
    LEFT JOIN Team_Wins_Draws_Losses HTWDL ON M.home_team_api_id = HTWDL.team_api_id
    LEFT JOIN Team_Wins_Draws_Losses ATWDL ON M.away_team_api_id = ATWDL.team_api_id
    ORDER BY Date DESC
    '''

    finalTable = pd.read_sql_query(query, conn)

    cursor.execute('''
                   DROP TABLE home_team_attributes
                   ''')
                   
    cursor.execute('''
                   DROP TABLE away_team_attributes
                   ''')
                   
    return finalTable



def GetGroupOfCoordinates(tbl):
    '''Creates columns, which contain the home and away players' coordinates.'''

    tbl["homeXCoords"] = tbl.apply(lambda row: [ row['home_player_X1'] , 
                                                 row["home_player_X2"] ,
                                                 row['home_player_X3'] , 
                                                 row["home_player_X4"] ,
                                                 row['home_player_X5'] , 
                                                 row["home_player_X6"] ,
                                                 row['home_player_X7'] , 
                                                 row["home_player_X8"] ,
                                                 row['home_player_X9'] , 
                                                 row["home_player_X10"] ,
                                                 row['home_player_X11']] , axis = 1 )
    
    tbl["homeYCoords"] = tbl.apply(lambda row: [ row['home_player_Y1'] , 
                                                 row["home_player_Y2"] ,
                                                 row['home_player_Y3'] , 
                                                 row["home_player_Y4"] ,
                                                 row['home_player_Y5'] , 
                                                 row["home_player_Y6"] ,
                                                 row['home_player_Y7'] , 
                                                 row["home_player_Y8"] ,
                                                 row['home_player_Y9'] , 
                                                 row["home_player_Y10"] ,
                                                 row['home_player_Y11']] , axis = 1 )
    
    tbl["awayXCoords"] = tbl.apply(lambda row: [ row['away_player_X1'] , 
                                                 row["away_player_X2"] ,
                                                 row['away_player_X3'] , 
                                                 row["away_player_X4"] ,
                                                 row['away_player_X5'] , 
                                                 row["away_player_X6"] ,
                                                 row['away_player_X7'] , 
                                                 row["away_player_X8"] ,
                                                 row['away_player_X9'] , 
                                                 row["away_player_X10"] ,
                                                 row['away_player_X11']] , axis = 1 )
    
    tbl["awayYCoords"] = tbl.apply(lambda row: [ row['away_player_Y1'] , 
                                                 row["away_player_Y2"] ,
                                                 row['away_player_Y3'] , 
                                                 row["away_player_Y4"] ,
                                                 row['away_player_Y5'] , 
                                                 row["away_player_Y6"] ,
                                                 row['away_player_Y7'] , 
                                                 row["away_player_Y8"] ,
                                                 row['away_player_Y9'] , 
                                                 row["away_player_Y10"] ,
                                                 row['away_player_Y11']] , axis = 1 )
        
    return tbl


def DropUnnecessaryColumns(tbl):
    '''After creating the columns that groups players and their coordinates, it drops the columns which were used to do this job.'''


    tbl = tbl.drop(['home_player_X1', 
                    'home_player_X2',
                    'home_player_X3',
                    'home_player_X4',
                    'home_player_X5',
                    'home_player_X6',
                    'home_player_X7',
                    'home_player_X8',
                    'home_player_X9',
                    'home_player_X10',
                    'home_player_X11',
                    'home_player_Y1',
                    'home_player_Y2',
                    'home_player_Y3',
                    'home_player_Y4',
                    'home_player_Y5',
                    'home_player_Y6',
                    'home_player_Y7',
                    'home_player_Y8',
                    'home_player_Y9',
                    'home_player_Y10',
                    'home_player_Y11',
                    'away_player_X1', 
                    'away_player_X2',
                    'away_player_X3',
                    'away_player_X4',
                    'away_player_X5',
                    'away_player_X6',
                    'away_player_X7',
                    'away_player_X8',
                    'away_player_X9',
                    'away_player_X10',
                    'away_player_X11',
                    'away_player_Y1',
                    'away_player_Y2',
                    'away_player_Y3',
                    'away_player_Y4',
                    'away_player_Y5',
                    'away_player_Y6',
                    'away_player_Y7',
                    'away_player_Y8',
                    'away_player_Y9',
                    'away_player_Y10',
                    'away_player_Y11',
                    'home_player_1',
                    'home_player_2',
                    'home_player_3',
                    'home_player_4',
                    'home_player_5',
                    'home_player_6',
                    'home_player_7',
                    'home_player_8',
                    'home_player_9',
                    'home_player_10',
                    'home_player_11',
                    'away_player_1', 
                    'away_player_2',
                    'away_player_3',
                    'away_player_4',
                    'away_player_5',
                    'away_player_6',
                    'away_player_7',
                    'away_player_8',
                    'away_player_9',
                    'away_player_10',
                    'away_player_11',], axis=1)
    
    return tbl



def GetTeamForm(tbl):
    '''Calculates both of the teams' form from the last 10 games.
       Example: WDDLWWLLWW      W=Win, D=Draw, L=Loss'''

    tbl["home_team_form"] = ""  
    tbl["away_team_form"] = ""
    for index, row in tbl.iterrows():
        
        homeId = row["home_team_api_id"]
        awayId = row["away_team_api_id"]
        matchDate = row["date"]
        
        home_team_games = tbl[((tbl['home_team_api_id'] == homeId) | (tbl['away_team_api_id'] == homeId)) & (tbl['date'] < matchDate)]
        home_team_games = home_team_games.head(10)
        away_team_games = tbl[((tbl['home_team_api_id'] == awayId) | (tbl['away_team_api_id'] == awayId)) & (tbl['date'] < matchDate)]
        away_team_games = away_team_games.head(10)
        
        for _, row2 in home_team_games.iterrows():
            if row2['home_team_api_id'] == homeId:
                if row2['result'] == 1:
                    tbl.at[ index , 'home_team_form' ] += 'W'
                elif row2['result'] == 0:
                    tbl.at[ index , 'home_team_form' ] += 'D'
                else:
                    tbl.at[ index , 'home_team_form' ] += 'L'
            else:
                if row2['result'] == 1:
                    tbl.at[ index , 'home_team_form' ] += 'L'
                elif row2['result'] == 0:
                    tbl.at[ index , 'home_team_form' ] += 'D'
                else:
                    tbl.at[ index , 'home_team_form' ] += 'W'
        
        
        for _, row2 in away_team_games.iterrows():
            if row2['away_team_api_id'] == awayId:
                if row2['result'] == 1:
                    tbl.at[ index , 'away_team_form' ] += 'W'
                elif row2['result'] == 0:
                    tbl.at[ index , 'away_team_form' ] += 'D'
                else:
                    tbl.at[ index , 'away_team_form' ] += 'L'
            else:
                if row2['result'] == 1:
                    tbl.at[ index , 'away_team_form' ] += 'L'
                elif row2['result'] == 0:
                    tbl.at[ index , 'away_team_form' ] += 'D'
                else:
                    tbl.at[ index , 'away_team_form' ] += 'W'
    
    return tbl

def GetAgainstEachOtherResults(tbl):
    '''Calculates the total wins/draws/losses between the 2 teams.'''
    
    tbl["home_wins_against_away"] = 0
    tbl["home_draws_against_away"] = 0
    tbl["away_wins_against_home"] = 0
    
    for index, row in tbl.iterrows():
        
        homeId = row["home_team_api_id"]
        awayId = row["away_team_api_id"]
        matchDate = row["date"]
        
        home_away_games = tbl[((tbl['home_team_api_id'] == homeId) | 
                               (tbl['away_team_api_id'] == homeId)) & 
                               ((tbl['home_team_api_id'] == awayId) | 
                               (tbl['away_team_api_id'] == awayId))]
        
        for _, row2 in home_away_games.iterrows():
            if row2['home_team_api_id'] == homeId:
                if row2['result'] == 1:
                    tbl.at[ index , 'home_wins_against_away' ] += 1
                elif row2['result'] == 0:
                    tbl.at[ index , 'home_draws_against_away' ] += 1
                else:
                    tbl.at[ index , 'away_wins_against_home' ] += 1
            else:
                if row2['result'] == 1:
                    tbl.at[ index , 'away_wins_against_home' ] += 1
                elif row2['result'] == 0:
                    tbl.at[ index , 'home_draws_against_away' ] += 1
                else:
                    tbl.at[ index , 'home_wins_against_away' ] += 1
                    
    return tbl
        

def GetTeamPlayers(tbl):
    '''Groups home and away players into 2 seperate columns.'''

    tbl["home_team_players"] = tbl.apply(lambda row: [ row['home_player_1'] , 
                                                 row["home_player_2"] ,
                                                 row['home_player_3'] , 
                                                 row["home_player_4"] ,
                                                 row['home_player_5'] , 
                                                 row["home_player_6"] ,
                                                 row['home_player_7'] , 
                                                 row["home_player_8"] ,
                                                 row['home_player_9'] , 
                                                 row["home_player_10"] ,
                                                 row['home_player_11']] , axis = 1 )
    
    tbl["away_team_players"] = tbl.apply(lambda row: [ row['away_player_1'] , 
                                                 row["away_player_2"] ,
                                                 row['away_player_3'] , 
                                                 row["away_player_4"] ,
                                                 row['away_player_5'] , 
                                                 row["away_player_6"] ,
                                                 row['away_player_7'] , 
                                                 row["away_player_8"] ,
                                                 row['away_player_9'] , 
                                                 row["away_player_10"] ,
                                                 row['away_player_11']] , axis = 1 )
    
    return tbl

    
def GetTeamGroups(tbl):
    '''Seperates players into 3 groups (Defence, Midfield, Attack), according to their coordinates.'''
    
    
    
    tbl["home_players_with_coords"] = [ {} for _ in range(len(tbl)) ]
    tbl["away_players_with_coords"] = [ {} for _ in range(len(tbl)) ]
    
    for index, row in tbl.iterrows():
        tbl.at[ index, 'home_players_with_coords'] = { k: [ v1, v2 ] for k, v1, v2 in list(zip(row['home_team_players'] , row['homeXCoords'] , row['homeYCoords']))}
        tbl.at[ index, 'away_players_with_coords'] = { k: [ v1, v2 ] for k, v1, v2 in list(zip(row['away_team_players'] , row['awayXCoords'] , row['awayYCoords']))}


    tbl["home_defence"] = [[k for k, v in player_dict.items() if v[1] <= 4] for player_dict in tbl['home_players_with_coords']]
    tbl["home_midfield"] = [[k for k, v in player_dict.items() if 4 < v[1] <= 8] for player_dict in tbl['home_players_with_coords']]
    tbl["home_attack"] = [[k for k, v in player_dict.items() if v[1] > 8] for player_dict in tbl['home_players_with_coords']]
    tbl["away_defence"] = [[k for k, v in player_dict.items() if v[1] <= 4] for player_dict in tbl['away_players_with_coords']]
    tbl["away_midfield"] = [[k for k, v in player_dict.items() if 4 < v[1] <= 8] for player_dict in tbl['away_players_with_coords']]
    tbl["away_attack"] = [[k for k, v in player_dict.items() if v[1] > 8] for player_dict in tbl['away_players_with_coords']]
    
    return tbl        
        

    
def GetOverallRating(tbl, pa):
    '''Get the overall rating for each team, using the average of players' overall ratings.
       Players' overall ratings exist in Player_attributes table. Each player has more than one rows in this table, as there are more than one dates.
       This function gets player's data which are more closely to the date of the match.
    '''
    warnings.filterwarnings('ignore')
    
    tbl['date'] = pd.to_datetime(tbl['date'])
    pa['date'] = pd.to_datetime(pa['date'])
    
    tbl['home_team_OR'] = [ {} for _ in range(len(tbl)) ]
    tbl['away_team_OR'] = [ {} for _ in range(len(tbl)) ]
    
    for index, row in tbl.iterrows():
        
        rowDate = row['date']
        
        homeFiltered = pa[pa.player_api_id.isin(row['home_team_players'])]
        homeFiltered.loc[ : , 'date_diff'] = abs(homeFiltered['date'] - row['date'])
        homeSorted = homeFiltered.sort_values(by='date_diff')
        homeResult = homeSorted.groupby('player_api_id').first().reset_index()
        
        awayFiltered = pa[pa.player_api_id.isin(row['away_team_players'])]
        awayFiltered.loc[ : , 'date_diff'] = abs(awayFiltered['date'] - row['date'])
        awaySorted = awayFiltered.sort_values(by='date_diff')
        awayResult = awaySorted.groupby('player_api_id').first().reset_index()
        
        tbl.at[ index, 'home_team_OR' ] = homeResult.set_index('player_api_id')['overall_rating'].to_dict()
        tbl.at[ index, 'away_team_OR' ] = awayResult.set_index('player_api_id')['overall_rating'].to_dict()
    
    
    return tbl       
        
        

def calculate_average_rating(d, keys):
    '''Calculate average rating of a dictionary'''
    
    filtered_dict = {k: v for k, v in d.items() if k in keys}
    
    if len(filtered_dict) == 0:
        return 0
    else:
        return round(sum(filtered_dict.values()) / len(filtered_dict) , 2)
        
def GetAvgRating(tbl):
    '''Calculates the players' average rating of the whole team and the of the 3 groups (Defence, Midfield, Attack)'''
    
    tbl['home_team_overall_rating'] = tbl['home_team_OR'].apply(lambda x: round(sum(x.values()) / len(x) , 2) if len(x) > 0 else 0)
    tbl['away_team_overall_rating'] = tbl['away_team_OR'].apply(lambda x: round(sum(x.values()) / len(x) , 2) if len(x) > 0 else 0)
    
    tbl['home_defence_rating'] = tbl.apply(lambda row: calculate_average_rating(row['home_team_OR'], row['home_defence']), axis=1)
    tbl['home_midfield_rating'] = tbl.apply(lambda row: calculate_average_rating(row['home_team_OR'], row['home_midfield']), axis=1)
    tbl['home_attack_rating'] = tbl.apply(lambda row: calculate_average_rating(row['home_team_OR'], row['home_attack']), axis=1)
    tbl['away_defence_rating'] = tbl.apply(lambda row: calculate_average_rating(row['away_team_OR'], row['away_defence']), axis=1)
    tbl['away_midfield_rating'] = tbl.apply(lambda row: calculate_average_rating(row['away_team_OR'], row['away_midfield']), axis=1)
    tbl['away_attack_rating'] = tbl.apply(lambda row: calculate_average_rating(row['away_team_OR'], row['away_attack']), axis=1)
    
    return tbl
        
        
def DropUnusefulRows(tbl):
    '''Drops unuseful rows, in which there are a lot of NULL values.'''

    tbl.dropna(subset = [ 'home_player_1' , 'goal' ], inplace = True)
    return tbl



def GetAvgAgeHeightWeight(tbl):
    '''Calculates the average age/height/weight of team's players.'''
    
    tbl['home_avg_age'] = round(tbl[['home_player_1_age' ,
                               'home_player_2_age' ,
                               'home_player_3_age' ,
                               'home_player_4_age' ,
                               'home_player_5_age' ,
                               'home_player_6_age' ,
                               'home_player_7_age' ,
                               'home_player_8_age' ,
                               'home_player_9_age' ,
                               'home_player_10_age' ,
                               'home_player_11_age']].mean(axis = 1) , 1)
    
    tbl['home_avg_height'] = round(tbl[['home_player_1_height' ,
                               'home_player_2_height' ,
                               'home_player_3_height' ,
                               'home_player_4_height' ,
                               'home_player_5_height' ,
                               'home_player_6_height' ,
                               'home_player_7_height' ,
                               'home_player_8_height' ,
                               'home_player_9_height' ,
                               'home_player_10_height' ,
                               'home_player_11_height']].mean(axis = 1) , 1)
    
    tbl['home_avg_weight'] = round(tbl[['home_player_1_weight' ,
                               'home_player_2_weight' ,
                               'home_player_3_weight' ,
                               'home_player_4_weight' ,
                               'home_player_5_weight' ,
                               'home_player_6_weight' ,
                               'home_player_7_weight' ,
                               'home_player_8_weight' ,
                               'home_player_9_weight' ,
                               'home_player_10_weight' ,
                               'home_player_11_weight']].mean(axis = 1) , 1)
    
    tbl['away_avg_age'] = round(tbl[['away_player_1_age' ,
                               'away_player_2_age' ,
                               'away_player_3_age' ,
                               'away_player_4_age' ,
                               'away_player_5_age' ,
                               'away_player_6_age' ,
                               'away_player_7_age' ,
                               'away_player_8_age' ,
                               'away_player_9_age' ,
                               'away_player_10_age' ,
                               'away_player_11_age']].mean(axis = 1) , 1)
    
    tbl['away_avg_height'] = round(tbl[['away_player_1_height' ,
                               'away_player_2_height' ,
                               'away_player_3_height' ,
                               'away_player_4_height' ,
                               'away_player_5_height' ,
                               'away_player_6_height' ,
                               'away_player_7_height' ,
                               'away_player_8_height' ,
                               'away_player_9_height' ,
                               'away_player_10_height' ,
                               'away_player_11_height']].mean(axis = 1) , 1)
    
    tbl['away_avg_weight'] = round(tbl[['away_player_1_weight' ,
                               'away_player_2_weight' ,
                               'away_player_3_weight' ,
                               'away_player_4_weight' ,
                               'away_player_5_weight' ,
                               'away_player_6_weight' ,
                               'away_player_7_weight' ,
                               'away_player_8_weight' ,
                               'away_player_9_weight' ,
                               'away_player_10_weight' ,
                               'away_player_11_weight']].mean(axis = 1) , 1)
    
    return tbl


def FixZeroRating(tbl):
    '''Replaces 0 rating with the whole team's average rating.'''

    tbl.loc[tbl['home_defence_rating'] == 0 , 'home_defence_rating'] = tbl.loc[tbl['home_defence_rating'] == 0, 'home_team_overall_rating']
    tbl.loc[tbl['home_midfield_rating'] == 0 , 'home_midfield_rating'] = tbl.loc[tbl['home_midfield_rating'] == 0, 'home_team_overall_rating']
    tbl.loc[tbl['home_attack_rating'] == 0 , 'home_attack_rating'] = tbl.loc[tbl['home_attack_rating'] == 0, 'home_team_overall_rating']
    tbl.loc[tbl['away_defence_rating'] == 0 , 'away_defence_rating'] = tbl.loc[tbl['away_defence_rating'] == 0, 'away_team_overall_rating']
    tbl.loc[tbl['away_midfield_rating'] == 0 , 'away_midfield_rating'] = tbl.loc[tbl['away_midfield_rating'] == 0, 'away_team_overall_rating']
    tbl.loc[tbl['away_attack_rating'] == 0 , 'away_attack_rating'] = tbl.loc[tbl['away_attack_rating'] == 0, 'away_team_overall_rating']
    
    return tbl


def ExtractXMLPossession(xml_string):
    '''Gets the home and away team possession from the corresponding XML column'''

    if xml_string is None:
        return None, None
    
    soup = BeautifulSoup(xml_string, 'lxml')
    pos_values = soup.find_all('value')

    awaypos = None
    homepos = None

    for pos in pos_values:
        awayposElem = pos.find('awaypos')
        homeposElem = pos.find('homepos')

        if not awayposElem:
            awaypos = None
        else:
            awaypos = int(awayposElem.text)

        if not homeposElem:
            homepos = None
        else:
            homepos = int(homeposElem.text)

    if awaypos is None:
        if homepos is not None:
            return 100 - homepos , homepos
        else:
            return None , None
    else:
        if homepos is not None:
            return awaypos , homepos
        else:
            return awaypos , 100 - awaypos
            
     

def ExtractXMLRemainingData(xmlString , typeOfStat):
    '''Gets the home and away team shots on target, shots of target, corners, crosses and fouls committed from the corresponding XML columns'''

    if xmlString is None:
        return {}
    
    soup = BeautifulSoup(xmlString, 'lxml')
    pos_values = soup.find_all('value')
    #shotData = {}
    elemList = []
    for pos in pos_values:
       elem = pos.find(typeOfStat)
       teamElem = pos.find('team')
       
       if not elem:
           elemValue = 0
       else:
           elemValue = int(elem.text)
       
       if teamElem:
           team = int(teamElem.text)
           elemList.append((elemValue , team))


    counts = {}   
    if len(elemList) > 0:
        for _, num in elemList:
            counts[num] = counts.get(num, 0) + 1
    
       
    return counts

def ExtractXMLCards(xmlString , typeOfStat):
    '''Gets the home and away team yellow and red cards from the corresponding XML column'''

    if xmlString is None:
        return {}
    
    
    soup = BeautifulSoup(xmlString, 'lxml')
    pos_values = soup.find_all('value')
    #cardData = {}
    cardList = []
    for pos in pos_values:
       cardElem = pos.find(typeOfStat)
       teamElem = pos.find('team')
       
       if not cardElem:
           card = 0
       else:
           card = int(cardElem.text)
       
       if teamElem:
           team = int(teamElem.text)
           cardList.append((card , team))


    counts = {}   
    if len(cardList) > 0:
        for cardsNumber, teamId in cardList:
            if cardsNumber == 1:
                counts[teamId] = counts.get(teamId, 0) + 1
    
    return counts

def FixPossessionNulls(tbl):
    '''Replaces Null values in possession with the mean value.'''

    mean_possession_by_result = round(tbl.groupby('result')['home_possession'].transform('mean'),2)
    null_mask = tbl['home_possession'].isnull()
    null_rows = tbl[null_mask]
    tbl.loc[null_mask, 'home_possession'] = null_rows['result'].map(mean_possession_by_result)

    mean_possession_by_result = round(tbl.groupby('result')['away_possession'].transform('mean'),2)
    null_mask = tbl['away_possession'].isnull()
    null_rows = tbl[null_mask]
    tbl.loc[null_mask, 'away_possession'] = null_rows['result'].map(mean_possession_by_result)
    return tbl




def ExtractXMLData(tbl):
    '''Create new columns according to the dataset's XML columns'''

    #possession
    print("Calculating XML possession...")
    tbl[[ 'away_possession' , 'home_possession' ]] = tbl['possession'].apply(lambda x: pd.Series(ExtractXMLPossession(x)))
    tbl = FixPossessionNulls(tbl)
    
    #shoton
    print("Calculating XML shots on...")
    tbl['shoton_data'] = tbl['shoton'].apply(ExtractXMLRemainingData , typeOfStat = 'stats/shoton')
    tbl[['home_team_shoton', 'away_team_shoton']] = tbl.apply(lambda row: (row['shoton_data'][row['home_team_api_id']] if row['home_team_api_id'] in row['shoton_data'] else row['home_team_goal'],
                                                                           row['shoton_data'][row['away_team_api_id']] if row['away_team_api_id'] in row['shoton_data'] else row['away_team_goal']), 
                                                              axis=1, result_type='expand')
    tbl = tbl.drop(columns=['shoton_data'])
    
    #shotoff
    print("Calculating XML shots off...")
    tbl['shotoff_data'] = tbl['shotoff'].apply(ExtractXMLRemainingData , typeOfStat = 'stats/shotoff')
    tbl[['home_team_shotoff', 'away_team_shotoff']] = tbl.apply(lambda row: (row['shotoff_data'][row['home_team_api_id']] if row['home_team_api_id'] in row['shotoff_data'] else 0,
                                                                           row['shotoff_data'][row['away_team_api_id']] if row['away_team_api_id'] in row['shotoff_data'] else 0), 
                                                              axis=1, result_type='expand')
    tbl = tbl.drop(columns=['shotoff_data'])
    
    #foulcommit
    print("Calculating XML fouls committed...")
    tbl['foulcommit_data'] = tbl['foulcommit'].apply(ExtractXMLRemainingData , typeOfStat = 'stats/foulscommitted')
    tbl[['home_team_foulcommit', 'away_team_foulcommit']] = tbl.apply(lambda row: (row['foulcommit_data'][row['home_team_api_id']] if row['home_team_api_id'] in row['foulcommit_data'] else 0,
                                                                           row['foulcommit_data'][row['away_team_api_id']] if row['away_team_api_id'] in row['foulcommit_data'] else 0), 
                                                              axis=1, result_type='expand')
    tbl = tbl.drop(columns=['foulcommit_data'])
    
    #cross
    print("Calculating XML crosses...")
    tbl['cross_data'] = tbl['cross'].apply(ExtractXMLRemainingData , typeOfStat = 'stats/crosses')
    tbl[['home_team_crosses', 'away_team_crosses']] = tbl.apply(lambda row: (row['cross_data'][row['home_team_api_id']] if row['home_team_api_id'] in row['cross_data'] else 0,
                                                                           row['cross_data'][row['away_team_api_id']] if row['away_team_api_id'] in row['cross_data'] else 0), 
                                                              axis=1, result_type='expand')
    tbl = tbl.drop(columns=['cross_data'])
    
    #corner
    print("Calculating XML corners...")
    tbl['corner_data'] = tbl['corner'].apply(ExtractXMLRemainingData , typeOfStat = 'stats/corners')
    tbl[['home_team_corners', 'away_team_corners']] = tbl.apply(lambda row: (row['corner_data'][row['home_team_api_id']] if row['home_team_api_id'] in row['corner_data'] else 0,
                                                                           row['corner_data'][row['away_team_api_id']] if row['away_team_api_id'] in row['corner_data'] else 0), 
                                                              axis=1, result_type='expand')
    tbl = tbl.drop(columns=['corner_data'])
    
        #yellow cards
    print("Calculating XML yellow cards...")
    tbl['ycard_data'] = tbl['card'].apply(ExtractXMLCards , typeOfStat = 'ycards')
    tbl[['home_team_ycards', 'away_team_ycards']] = tbl.apply(lambda row: (row['ycard_data'][row['home_team_api_id']] if row['home_team_api_id'] in row['ycard_data'] else 0,
                                                                           row['ycard_data'][row['away_team_api_id']] if row['away_team_api_id'] in row['ycard_data'] else 0), 
                                                              axis=1, result_type='expand')
    
    tbl = tbl.drop(columns=['ycard_data'])
    
    print("Calculating XML red cards...")
    tbl['rcard_data'] = tbl['card'].apply(ExtractXMLCards , typeOfStat = 'rcards')
    tbl[['home_team_rcards', 'away_team_rcards']] = tbl.apply(lambda row: (row['rcard_data'][row['home_team_api_id']] if row['home_team_api_id'] in row['rcard_data'] else 0,
                                                                           row['rcard_data'][row['away_team_api_id']] if row['away_team_api_id'] in row['rcard_data'] else 0), 
                                                              axis=1, result_type='expand')
    
    tbl = tbl.drop(columns=['rcard_data'])
    
    return tbl


def GetFinalInput(tbl):
    '''Creates the 1st version of input.'''

    finalInput = tbl.loc[ : , [ 'home_team_api_id',
                                'away_team_api_id',  
                                'home_buildUpPlaySpeed',     
                                'home_buildUpPlayDribbling',                               
                                'home_buildUpPlayPassing',                               
                                'home_buildUpPlayPositioning',
                                'home_chanceCreationPassing',                               
                                'home_chanceCreationCrossing',                               
                                'home_chanceCreationShooting',                               
                                'home_chanceCreationPositioning',
                                'home_defencePressure',                               
                                'home_defenceAggression',                               
                                'home_defenceTeamWidth',                               
                                'home_defenceDefenderLine',
                                'away_buildUpPlaySpeed',                                
                                'away_buildUpPlayDribbling',                               
                                'away_buildUpPlayPassing',                                
                                'away_buildUpPlayPositioning',
                                'away_chanceCreationPassing',                                
                                'away_chanceCreationCrossing',                               
                                'away_chanceCreationShooting',
                                'away_chanceCreationPositioning',
                                'away_defencePressure',
                                'away_defenceAggression',
                                'away_defenceTeamWidth',
                                'away_defenceDefenderLine',
                                'home_team_total_goals_scored',
                                'away_team_total_goals_scored',
                                'home_team_total_goals_conceded',
                                'away_team_total_goals_conceded',
                                'home_team_total_goals_diff',
                                'away_team_total_goals_diff',
                                'home_team_total_wins',
                                'home_team_total_draws',
                                'home_team_total_losses',
                                'away_team_total_wins',
                                'away_team_total_draws',
                                'away_team_total_losses',
                                'home_wins_against_away',
                                'home_draws_against_away',
                                'away_wins_against_home',
                                'home_team_overall_rating',
                                'away_team_overall_rating',
                                'home_defence_rating',
                                'home_midfield_rating',
                                'home_attack_rating',
                                'away_defence_rating',
                                'away_midfield_rating',
                                'away_attack_rating',
                                'home_avg_age',
                                'home_avg_height',
                                'home_avg_weight',
                                'away_avg_age',
                                'away_avg_height',
                                'away_avg_weight',
                                'home_possession',
                                'away_possession',
                                'home_team_shoton',
                                'away_team_shoton',
                                'home_team_shotoff',
                                'away_team_shotoff',
                                'home_team_crosses',
                                'away_team_crosses',
                                'home_team_corners',
                                'away_team_corners',
                                'home_team_foulcommit',
                                'away_team_foulcommit',
                                'home_team_ycards',
                                'away_team_ycards',
                                'home_team_rcards',
                                'away_team_rcards',
                                'result'] ]
    
    return finalInput

def MergeColumns(tbl):
    '''Merges numerical columns by substracting away values from home values. After that, it drops every home_ and away_ column.'''

    tbl['buildUpPlaySpeed'] = tbl['home_buildUpPlaySpeed'] - tbl['away_buildUpPlaySpeed']
    tbl['buildUpPlayPassing'] = tbl['home_buildUpPlayPassing'] - tbl['away_buildUpPlayPassing']
    tbl['chanceCreationPassing'] = tbl['home_chanceCreationPassing'] - tbl['away_chanceCreationPassing']
    tbl['chanceCreationCrossing'] = tbl['home_chanceCreationCrossing'] - tbl['away_chanceCreationCrossing']
    tbl['chanceCreationShooting'] = tbl['home_chanceCreationShooting'] - tbl['away_chanceCreationShooting']
    tbl['defencePressure'] = tbl['home_defencePressure'] - tbl['away_defencePressure']
    tbl['defenceAggression'] = tbl['home_defenceAggression'] - tbl['away_defenceAggression']
    tbl['defenceTeamWidth'] = tbl['home_defenceTeamWidth'] - tbl['away_defenceTeamWidth']
    tbl['totalGoalsScoredDiff'] = tbl['home_team_total_goals_scored'] - tbl['away_team_total_goals_scored']
    tbl['totalGoalsConcededDiff'] = tbl['home_team_total_goals_conceded'] - tbl['away_team_total_goals_conceded']
    tbl['goalDiff'] = tbl['home_team_total_goals_diff'] - tbl['away_team_total_goals_diff']
    tbl['pointsDiff'] = tbl['home_team_points'] = tbl['away_team_points']
    tbl['overallRatingDiff'] = tbl['home_team_overall_rating'] - tbl['away_team_overall_rating']
    tbl['defenceRatingDiff'] = tbl['home_defence_rating'] - tbl['away_defence_rating']
    tbl['midfieldRatingDiff'] = tbl['home_midfield_rating'] - tbl['away_midfield_rating']
    tbl['attackRatingDiff'] = tbl['home_attack_rating'] - tbl['away_attack_rating']
    tbl['ageDiff'] = tbl['home_avg_age'] - tbl['away_avg_age']
    tbl['heightDiff'] = tbl['home_avg_height'] - tbl['away_avg_height']
    tbl['weightDiff'] = tbl['home_avg_weight'] - tbl['away_avg_weight']
    tbl['possessionDiff'] = tbl['home_possession'] - tbl['away_possession']
    tbl['shotOnDiff'] = tbl['home_team_shoton'] - tbl['away_team_shoton']
    tbl['shotOffDiff'] = tbl['home_team_shotoff'] - tbl['away_team_shotoff']
    tbl['crossesDiff'] = tbl['home_team_crosses'] - tbl['away_team_crosses']
    tbl['cornersDiff'] = tbl['home_team_corners'] - tbl['away_team_corners']
    tbl['foulCommitDiff'] = tbl['home_team_foulcommit'] - tbl['away_team_foulcommit']
    tbl['yCardsDiff'] = tbl['home_team_ycards'] - tbl['away_team_ycards']
    tbl['rCardsDiff'] = tbl['home_team_rcards'] - tbl['away_team_rcards']
    

    columnsToDrop =    ['home_buildUpPlaySpeed',                                                              
                        'home_buildUpPlayPassing',                               
                        'home_chanceCreationPassing',                               
                        'home_chanceCreationCrossing',                               
                        'home_chanceCreationShooting',                               
                        'home_defencePressure',                               
                        'home_defenceAggression',                               
                        'home_defenceTeamWidth',                               
                        'away_buildUpPlaySpeed',                                                             
                        'away_buildUpPlayPassing',                                
                        'away_chanceCreationPassing',                                
                        'away_chanceCreationCrossing',                               
                        'away_chanceCreationShooting',
                        'away_defencePressure',
                        'away_defenceAggression',
                        'away_defenceTeamWidth',
                        'home_team_total_goals_scored',
                        'away_team_total_goals_scored',
                        'home_team_total_goals_conceded',
                        'away_team_total_goals_conceded',
                        'home_team_total_goals_diff',
                        'away_team_total_goals_diff',
                        'home_team_total_wins',
                        'home_team_total_draws',
                        'home_team_total_losses',
                        'away_team_total_wins',
                        'away_team_total_draws',
                        'away_team_total_losses',
                        'home_team_points',
                        'away_team_points',
                        'home_team_overall_rating',
                        'away_team_overall_rating',
                        'home_defence_rating',
                        'home_midfield_rating',
                        'home_attack_rating',
                        'away_defence_rating',
                        'away_midfield_rating',
                        'away_attack_rating',
                        'home_avg_age',
                        'home_avg_height',
                        'home_avg_weight',
                        'away_avg_age',
                        'away_avg_height',
                        'away_avg_weight',
                        'home_possession',
                        'away_possession',
                        'home_team_shoton',
                        'away_team_shoton',
                        'home_team_shotoff',
                        'away_team_shotoff',
                        'home_team_crosses',
                        'away_team_crosses',
                        'home_team_corners',
                        'away_team_corners',
                        'home_team_foulcommit',
                        'away_team_foulcommit',
                        'home_team_ycards',
                        'away_team_ycards',
                        'home_team_rcards',
                        'away_team_rcards']

    tbl = tbl.drop(columnsToDrop, axis=1)

    tbl = tbl.assign(result=tbl.pop('result'))
    return tbl

def HandleNullValues(tbl):
    '''Replaces NULL numerical values with the mean value and NULL categorical values with the most frequent value.'''

    #Update numerical NULL values
    tbl['home_buildUpPlaySpeed'] = tbl['home_buildUpPlaySpeed'].fillna(tbl['home_buildUpPlaySpeed'].mean())
    tbl['home_buildUpPlayPassing'] = tbl['home_buildUpPlayPassing'].fillna(tbl['home_buildUpPlayPassing'].mean())
    tbl['home_chanceCreationPassing'] = tbl['home_chanceCreationPassing'].fillna(tbl['home_chanceCreationPassing'].mean())
    tbl['home_chanceCreationCrossing'] = tbl['home_chanceCreationCrossing'].fillna(tbl['home_chanceCreationCrossing'].mean())
    tbl['home_chanceCreationShooting'] = tbl['home_chanceCreationShooting'].fillna(tbl['home_chanceCreationShooting'].mean())
    tbl['home_defencePressure'] = tbl['home_defencePressure'].fillna(tbl['home_defencePressure'].mean())
    tbl['home_defenceAggression'] = tbl['home_defenceAggression'].fillna(tbl['home_defenceAggression'].mean())
    tbl['home_defenceTeamWidth'] = tbl['home_defenceTeamWidth'].fillna(tbl['home_defenceTeamWidth'].mean())
    tbl['away_buildUpPlaySpeed'] = tbl['away_buildUpPlaySpeed'].fillna(tbl['away_buildUpPlaySpeed'].mean())
    tbl['away_buildUpPlayPassing'] = tbl['away_buildUpPlayPassing'].fillna(tbl['away_buildUpPlayPassing'].mean())
    tbl['away_chanceCreationPassing'] = tbl['away_chanceCreationPassing'].fillna(tbl['away_chanceCreationPassing'].mean())
    tbl['away_chanceCreationCrossing'] = tbl['away_chanceCreationCrossing'].fillna(tbl['away_chanceCreationCrossing'].mean())
    tbl['away_chanceCreationShooting'] = tbl['away_chanceCreationShooting'].fillna(tbl['away_chanceCreationShooting'].mean())
    tbl['away_defencePressure'] = tbl['away_defencePressure'].fillna(tbl['away_defencePressure'].mean())
    tbl['away_defenceAggression'] = tbl['away_defenceAggression'].fillna(tbl['away_defenceAggression'].mean())
    tbl['away_defenceTeamWidth'] = tbl['away_defenceTeamWidth'].fillna(tbl['away_defenceTeamWidth'].mean())

    #Update categorical NULL values
    tbl['home_buildUpPlayPositioning'] = tbl['home_buildUpPlayPositioning'].fillna('Organised')
    tbl['away_buildUpPlayPositioning'] = tbl['away_buildUpPlayPositioning'].fillna('Organised')
    tbl['home_chanceCreationPositioning'] = tbl['home_chanceCreationPositioning'].fillna('Organised')
    tbl['away_chanceCreationPositioning'] = tbl['away_chanceCreationPositioning'].fillna('Organised')
    tbl['home_defenceDefenderLine'] = tbl['home_defenceDefenderLine'].fillna('Cover')
    tbl['away_defenceDefenderLine'] = tbl['away_defenceDefenderLine'].fillna('Cover')

    #Drop columns with high number of NULLs
    columnsToDrop = ['home_buildUpPlayDribbling' , 'away_buildUpPlayDribbling' ]
    tbl = tbl.drop(columnsToDrop, axis=1)
    return tbl

def GatherPoints(tbl):
    '''Calculates the total points: win * 3 + draw'''

    tbl['home_team_points'] = tbl['home_team_total_wins'] * 3 + tbl['home_team_total_draws']
    tbl['away_team_points'] = tbl['away_team_total_wins'] * 3 + tbl['away_team_total_draws']

    return tbl

def DoScalingEncoding(tbl):
    '''Scales the numerical values and one-hot encodes categorical values.'''

    # select only the numerical columns
    num_cols = tbl.select_dtypes(include=['float', 'int']).columns
    num_cols = num_cols.drop(['result' , 'home_team_api_id' , 'away_team_api_id'])

    # normalize the numerical columns using MinMaxScaler
    scaler = MinMaxScaler()
    tbl[num_cols] = scaler.fit_transform(tbl[num_cols])
    tbl = pd.get_dummies(tbl, columns=['home_buildUpPlayPositioning', 'home_chanceCreationPositioning', 
                                        'home_defenceDefenderLine', 'away_buildUpPlayPositioning', 
                                        'away_chanceCreationPositioning', 'away_defenceDefenderLine'])
    
    tbl = tbl.assign(result=tbl.pop('result'))
    return tbl