import os
import glob
import sys
import pandas as pd
import logging

#configuring logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

MLB_DATA_DIR_PATH = os.path.join(os.path.dirname(__file__), 'MLB_DATA_2025')


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
        sys.exit(1)

    logger.info(f"Total number of unique games across the 2025 season found: {len(location_map)}")
    return location_map


def process_games(game_map):
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
        logger.info(f"Total number of games processed: {len(final_games_df)}")


def process_linescores(linescore_map):
    logger.info("Processing linescores files...")

    all_linescores_list = []

    for gamepk, paths in linescore_map.items():
        try:
            linescores_df = pd.read_csv(paths['linescores_csv'])

            linescores_df = linescores_df[linescores_df['gamePk'] == gamepk].copy()

            # Sort by inning and half to ensure correct order
            linescores_df = linescores_df.sort_values(by=['inning', 'half'], ascending=[True, True])

            # Calculating batting team score
            linescores_df['cummulative_score'] = linescores_df.groupby('battingteamid')['runs'].cumsum()
            # Ensuring that only the runs scored before the current half-inning are counted for the batting team score
            linescores_df['batting_team_score'] = linescores_df.groupby('battingteamid')['cummulative_score'].shift(1).fillna(0).astype(int)

        except Exception as e:
            logger.error(f"Error processing linescores for game with gamePk {gamepk}: {e}")
            

def main():

    mapped_csv_location = get_csv_locations()

    if not mapped_csv_location:
        logger.error("No valid game CSV locations found. Exiting.")
        return

    process_games(mapped_csv_location)
    process_linescores(mapped_csv_location)
    
if __name__ == "__main__":
    main()