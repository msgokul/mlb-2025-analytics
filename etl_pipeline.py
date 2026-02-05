import os
import glob
import sys
import pandas as pd
import logging

#configuring logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

MLB_DATA_DIR_PATH = os.path.join(os.path.dirname(__file__), 'MLB_DATA_2025')

print("MLB Data Directory Path:", MLB_DATA_DIR_PATH)

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

    return location_map

return_value = get_csv_locations()
print("CSV Locations Retrieved:", return_value)