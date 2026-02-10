import os
import glob
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
import logging

# Configuring logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Confiigurations
MLB_DATA_DIR_PATH = os.path.join(os.path.dirname(__file__), 'MLB_DATA_2025')
DB_URL = os.getenv('DATABASE_URL')

def create_db_engine():
    try:
        if not DB_URL:
            logger.error("DATABASE_URL not found in environment variables. Please set it in the .env file.")
            exit(1)
        return create_engine(DB_URL)
    except Exception as e:
        logger.error(f"Error creating database engine: {e}")
        exit(1)

# Function to get the locations of the CSV files for each game
def get_csv_locations():

    try:
        location_map = {}
        date_folders = sorted(glob.glob(os.path.join(MLB_DATA_DIR_PATH, '*')))

        for date_folder in date_folders:
            folder_date = os.path.basename(date_folder)
            sport_dir = os.path.join(date_folder, 'sport_1')

            games_csv = os.path.join(sport_dir, 'games.csv')

            if not os.path.exists(games_csv):
                logger.warning(f"games.csv not found in {sport_dir}, skipping this date folder.")
                continue

            try:
                games_df = pd.read_csv(games_csv)

                if games_df.empty:
                    logger.warning(f"games.csv in {sport_dir} is empty, skipping this date folder.")
                    continue

                for _, game_row in games_df.iterrows():
                    gamepk = game_row['gamePk']

                    location_map[gamepk] = {
                        'date_folder' : folder_date,
                        'games_csv': games_csv, 
                        'linescores_csv': os.path.join(sport_dir,'linescores.csv'),
                        'runners_csv': os.path.join(sport_dir,'runners.csv'),
                    }
            except Exception as e:
                logger.error(f"Error reading games.csv in {sport_dir}: {e}")
                continue
    
    except Exception as e:
        logger.error(f"Error in get_csv_locations: {e}")
        exit(1)
    logger.info(f"Total number of unique games across the 2025 season found: {len(location_map)}")
    return location_map


def process_games(game_map, engine):
    logger.info("Processing games files...")

    all_games_list = []

    for gamepk, paths in game_map.items():
        try:
            games_df = pd.read_csv(paths['games_csv'])
            game_row = games_df[games_df['gamePk'] == gamepk]
            if game_row.empty:
                logger.warning(f"Game with gamePk {gamepk} not found in games.csv at {paths['games_csv']}, skipping this game.")
                continue
            all_games_list.append(game_row)

        except Exception as e:
            logger.error(f"Error processing game with gamePk {gamepk}: {e}")
            continue

    if all_games_list:
        final_games_df = pd.concat(all_games_list, ignore_index=True)
        final_games_df.columns = [col.lower() for col in final_games_df.columns]
        final_games_df.to_sql('game', engine, if_exists='append', index=False, chunksize=1000)
        logger.info(f"Total number of games processed: {len(final_games_df)}")


def process_linescores(linescore_map, engine):
    logger.info("Processing linescores files...")

    all_linescores_list = []

    for gamepk, paths in linescore_map.items():
        try:
            linescores_df = pd.read_csv(paths['linescores_csv'])

            linescores_df = linescores_df[linescores_df['gamePk'] == gamepk].copy()

            # Sort by inning and half to ensure correct order
            linescores_df = linescores_df.sort_values(by=['inning', 'half'], ascending=[True, True])

            teams = linescores_df['battingteamid'].unique()
            if len(teams) != 2:
                logger.warning(f"Unexpected number of teams in linescores for game with gamePk {gamepk}. Expected 2 teams, found {len(teams)}. Skipping this game.")
                continue
            else:
                scores = []
                scores_diff = []

                team_scores = {t:0 for t in teams}

                for _, row in linescores_df.iterrows():
                    batting_team = row['battingteamid']
                    current_batting_score = team_scores[batting_team]

                    fielder = [t for t in teams if t!=batting_team]
                    current_fielding_score = team_scores[fielder[0]] if fielder else 0

                    scores.append(current_batting_score)
                    scores_diff.append(current_batting_score - current_fielding_score)

                    team_scores[batting_team] += row['runs']

                linescores_df['battingteam_score'] = scores
                linescores_df['battingteam_score_diff'] = scores_diff

                all_linescores_list.append(linescores_df)
             
        except Exception as e:
            logger.error(f"Error processing linescores for game with gamePk {gamepk}: {e}")
    
    
    if all_linescores_list:
        final_linescores_df = pd.concat(all_linescores_list, ignore_index=True)
        final_linescores_df.columns = [col.lower() for col in final_linescores_df.columns]
        final_linescores_df.to_sql('linescore', engine, if_exists='append', index=False, chunksize=1000)
        logger.info(f"Total number of linescore entries processed: {len(final_linescores_df)}")

def process_runners(runners_map, engine):
    logger.info("Processing runners files...")

    all_runners_list = []
    for gamepk, paths in runners_map.items():
        try:
            runners_df = pd.read_csv(paths['runners_csv'])
            if runners_df.empty:
                continue

            runners_df = runners_df[runners_df['gamePk'] == gamepk].copy()

            def normalize_base(val):
                if pd.isna(val) or val == '': return 'B'
                if '1' in str(val): return '1B'
                if '2' in str(val): return '2B'
                if '3' in str(val): return '3B'
                if 'score' in str(val).lower() or 'home' in str(val).lower(): return 'HM'
                return str(val)
            
            if 'originBase' in runners_df.columns:
                runners_df['startbase'] = runners_df['originBase'].apply(normalize_base)
            if 'start' in runners_df.columns:
                runners_df['startbase'] = runners_df['start'].apply(normalize_base)
            if 'end' in runners_df.columns:
                runners_df['endbase'] = runners_df['end'].apply(normalize_base)
            runners_df['reachedbase'] = runners_df['endbase']

            runners_df['is_risp'] = runners_df['startbase'].isin(['2B', '3B'])
            
            is_hr = runners_df['event'].str.lower().str.contains('home run|homerun',  na=False)

            runners_df['is_firsttothird'] = (
                                            (runners_df['startbase'] == '1B')
                                            & (runners_df['endbase'] == '3B')
                                              & (~is_hr)
                                            )
            
            runners_df['is_secondtohome'] = (
                                            (runners_df['startbase'] == '2B')
                                            & (runners_df['endbase'] == 'HM')
                                              & (~is_hr)
                                            )
            
            col_map = {
                'gamePk': 'gamepk',
                'atBatIndex': 'atbatindex',
                'playIndex': 'playindex',
                'runnerid': 'runnerid',
                'playId': 'playid',
                'runnerfullName': 'runnerfullname',
                'isOut':'is_out', 
                'event':'eventtype', 
                'movementReason':'movementreason',
            }

            runners_df = runners_df.rename(columns=col_map)

            cols_to_keep = ['gamepk', 'atbatindex', 'playindex', 
                            'runnerid', 'playid', 'runnerfullname',                              
                            'startbase', 'endbase', 'reachedbase', 
                            'is_out', 'eventtype', 'movementreason',
                            'is_risp', 'is_firsttothird', 'is_secondtohome']
            
            df = runners_df[cols_to_keep]
            all_runners_list.append(df)

        except Exception as e:
            logger.error(f"Error processing runners for game with gamePk {gamepk}: {e}")

    if all_runners_list:
        final_runners_df = pd.concat(all_runners_list, ignore_index=True)
        final_runners_df.drop_duplicates(
            subset=['gamepk', 'atbatindex', 'playindex', 'runnerid'],
            keep='last', 
            inplace=True
        )
        final_runners_df.columns = [col.lower() for col in final_runners_df.columns]
        final_runners_df.to_sql('runner_play', engine, if_exists='append', index=False, chunksize=1000)
        logger.info(f"Total number of runner entries processed: {len(final_runners_df)}")

def main():
    engine = create_db_engine()

    # Check db connection
    try:
        with engine.connect() as connection:
            logger.info("Successfully connected to the database.")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return
    
    csv_location_map = get_csv_locations()

    if not csv_location_map:
        logger.error("No valid game CSV locations found. Exiting.")
        return

    process_games(csv_location_map, engine)
    process_linescores(csv_location_map, engine)
    process_runners(csv_location_map, engine)
    
if __name__ == "__main__":
    main()