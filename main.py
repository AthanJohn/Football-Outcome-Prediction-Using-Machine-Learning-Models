import sqlite3
import pandas as pd
import input_functions
import train_models
import data_analysis
from sklearn.model_selection import train_test_split

while True:
    createCSV = input("Do you want to create the csv from scratch? [ y , n ] ")

    if  createCSV != 'y' and createCSV != 'n':
        print('Wrong input.')
    else:
        break

if createCSV == 'y':
    print('Gathering data from SQLite...')
    conn = sqlite3.connect('database.sqlite')
    
    cursor = conn.cursor()
    
    country = pd.read_sql_query(f"SELECT * FROM Country", conn)
    league = pd.read_sql_query(f"SELECT * FROM League", conn)
    match = pd.read_sql_query(f"SELECT * FROM Match", conn)
    player = pd.read_sql_query(f"SELECT * FROM Player", conn)
    playerAttributes = pd.read_sql_query(f"SELECT * FROM Player_Attributes", conn)
    team = pd.read_sql_query(f"SELECT * FROM Team", conn)
    teamAttributes = pd.read_sql_query(f"SELECT * FROM Team_Attributes", conn)
    
    print('Creating input table...')
    inputTable = input_functions.GetInputTable(cursor, conn)
    
    print('Closing connection with SQLite...')
    conn.close()

    print("Calculating against each other results...")
    inputTable = input_functions.GetAgainstEachOtherResults(inputTable)
    
    print("Dropping unuseful rows...")
    inputTable = input_functions.DropUnusefulRows(inputTable)
    
    print('Creating Coordinates...')
    inputTable = input_functions.GetGroupOfCoordinates(inputTable)
    
    print("Creating teams with players...")
    inputTable = input_functions.GetTeamPlayers(inputTable)

    print("Creating position groups (Defence, Midfield, Attack)...")
    inputTable = input_functions.GetTeamGroups(inputTable)
    
    print("Dropping unnecessary columns...")
    inputTable = input_functions.DropUnnecessaryColumns(inputTable)

    print("Adding players' overall rating...")
    inputTable = input_functions.GetOverallRating(inputTable, playerAttributes)

    print("Calculating average team and group rating...")
    inputTable = input_functions.GetAvgRating(inputTable)
    
    print("Calculating average team age, height and weight...")
    inputTable = input_functions.GetAvgAgeHeightWeight(inputTable)
    
    print("Fixing zero values in group average overall rating...")
    inputTable = input_functions.FixZeroRating(inputTable)
    
    print("Extracting data from XML columns...")
    inputTable = input_functions.ExtractXMLData(inputTable)
    
    print('Creating pre_input.csv...')
    inputTable.to_csv('pre_input.csv')

    print("Creating final ML model input...")
    finalInput = input_functions.GetFinalInput(inputTable)
    
    print('Creating raw_input.csv...')
    finalInput.to_csv('raw_input.csv', index=False)

    print('Handling NULL values...')
    finalInput = input_functions.HandleNullValues(finalInput)

    print('Gather points according to wins/draws/losses...')
    finalInput = input_functions.GatherPoints(finalInput)

    print('Merging home/away columns...')
    finalInput = input_functions.MergeColumns(finalInput)

    print('Creating final_input.csv...')
    finalInput.to_csv('final_input.csv', index=False)

    print('Doing scaling and encoding...')
    finalInput = input_functions.DoScalingEncoding(finalInput)

    print('Creating final_input1.csv...')
    finalInput.to_csv('final_input1.csv', index=False)
else:
    print('Reading from CSV file...')
    finalInput = pd.read_csv('final_input1.csv')

teamStatsColumns = ['buildUpPlaySpeed','buildUpPlayPassing','chanceCreationPassing', 'chanceCreationCrossing','chanceCreationShooting',
                    'defencePressure','defenceAggression','defenceTeamWidth','home_buildUpPlayPositioning_Free Form','home_buildUpPlayPositioning_Organised',
                    'away_buildUpPlayPositioning_Free Form','away_buildUpPlayPositioning_Organised','home_chanceCreationPositioning_Free Form',
                    'home_chanceCreationPositioning_Organised','away_chanceCreationPositioning_Free Form','away_chanceCreationPositioning_Organised',
                    'home_defenceDefenderLine_Offside Trap','home_defenceDefenderLine_Cover','away_defenceDefenderLine_Offside Trap','away_defenceDefenderLine_Cover',
                    'result']
teamRatingColumns = ['totalGoalsScoredDiff','totalGoalsConcededDiff','goalDiff','pointsDiff','overallRatingDiff',
                    'defenceRatingDiff','midfieldRatingDiff','attackRatingDiff','result']
playerStatsColumns = ['ageDiff','heightDiff','weightDiff','result']

data_analysis.CorrelationMatrix(teamStatsColumns, finalInput, 'TeamStatistics')
data_analysis.CorrelationMatrix(teamRatingColumns, finalInput, 'TeamRating')
data_analysis.CorrelationMatrix(playerStatsColumns, finalInput, 'PlayerStatistics')

dropColumnsTeamStatistics = ['home_buildUpPlayPositioning_Free Form' , 'away_buildUpPlayPositioning_Free Form' ,'home_chanceCreationPositioning_Free Form',
                             'away_chanceCreationPositioning_Free Form','home_defenceDefenderLine_Offside Trap','away_defenceDefenderLine_Offside Trap']

dropColumnsTeamRating = ['defenceRatingDiff' , 'midfieldRatingDiff' , 'attackRatingDiff']

finalInput = data_analysis.dropColumnsAfterCorrMatrix(dropColumnsTeamStatistics, finalInput)
finalInput = data_analysis.dropColumnsAfterCorrMatrix(dropColumnsTeamRating, finalInput)

X = finalInput.drop(['result' , 'home_team_api_id' , 'away_team_api_id'], axis=1)
y = finalInput['result']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42)

train_models.trainModelsAndVisualize(X_train, X_test, y_train, y_test)