'''
	This is the NHL crawler.  

Scattered throughout are TODO tips on what to look for.

Assume this job isn't expanding in scope, but pretend it will be pushed into production to run 
automomously.  So feel free to add anywhere (not hinted, this is where we see your though process..)
    * error handling where you see things going wrong.  
    * messaging for monitoring or troubleshooting
    * anything else you think is necessary to have for restful nights
'''
import logging
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
import boto3
import requests
import time
import pandas as pd
from botocore.config import Config
from dateutil.parser import parse as dateparse

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)

class NHLApi:
    SCHEMA_HOST = "https://statsapi.web.nhl.com/"
    VERSION_PREFIX = "api/v1"

    def __init__(self, base=None):
        self.base = base if base else f'{self.SCHEMA_HOST}/{self.VERSION_PREFIX}'


    def schedule(self, start_date: datetime, end_date: datetime) -> dict:
        ''' 
        returns a dict tree structure that is like
            "dates": [ 
                {
                    " #.. meta info, one for each requested date ",
                    "games": [
                        { #.. game info },
                        ...
                    ]
                },
                ...
            ]
        '''
        return self._get(self._url('schedule'), {'startDate': start_date.strftime('%Y-%m-%d'), 'endDate': end_date.strftime('%Y-%m-%d')})

    def boxscore(self, game_id):
        '''
        returns a dict tree structure that is like
           "teams": {
                "home": {
                    " #.. other meta ",
                    "players": {
                        $player_id: {
                            "person": {
                                "id": $int,
                                "fullName": $string,
                                #-- other info
                                "currentTeam": {
                                    "name": $string,
                                    #-- other info
                                },
                                "stats": {
                                    "skaterStats": {
                                        "assists": $int,
                                        "goals": $int,
                                        #-- other status
                                    }
                                    #-- ignore "goalieStats"
                                }
                            }
                        },
                        #...
                    }
                },
                "away": {
                    #... same as "home" 
                }
            }

            See tests/resources/boxscore.json for a real example response
        '''
        url = self._url(f'game/{game_id}/boxscore')
        return self._get(url)

    def _get(self, url, params=None):
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def _url(self, path):
        return f'{self.base}/{path}'

@dataclass
class StorageKey:
    
    # I am partitioning based on the game ID provided by the NHL's API
    # (the feature 'gamePk' from the schedule API)
    # this should provide distinct files for individual games
    # given that no 2 games should have the same
    # game ID.
    # Worst case can provide a storage key based on game date
    # and the away/home team IDs.

    gameid: str

    def key(self):
        ''' renders the s3 key for the given set of properties '''
        # TODO use the properties to return the s3 key
        return f'{self.gameid}.csv'

class Storage():
    def __init__(self, dest_bucket, s3_client):
        self._s3_client = s3_client
        self.bucket = dest_bucket

    def store_game(self, key: StorageKey, game_data) -> bool:
        self._s3_client.put_object(Bucket=self.bucket, Key=key.key(), Body=game_data)
        return True

class Crawler():
    def __init__(self, api: NHLApi, storage: Storage):
        self.api = api
        self.storage = storage

    def crawl(self, startDate: datetime, endDate: datetime):

        # Essentially, the crawler is pinging the API in the following way:
        # - intital schedule call
        # - individual boxscore call for all games in the schedule that has been retrieved
        # It is also saving data to S3 on every single game call, which is another source of pipeline error.
        # We want to create some sort of logger that tracks that the API has been successfully called
        # and the resulting data successfully saved to S3. 
        # 

        # get output of API call using start/end date
        logging.info(f'Initializing schedule load beginning {str(datetime.utcnow())}.')
        schedule_info = self.api.schedule(startDate,endDate)
        logging.info('Loaded schedule.')
        
        # The way this crawler retrieves data is by iterating through each game in each date,
        # getting the game ID, calling the boxscore using the game ID + boxscore parser defined above,
        # getting the player data (if the player had skaterStats)
        for date in schedule_info['dates']:
            logging.info('Loading games from ' + date['date'])
            for game in date['games']:

                # load boxscore
                game_id = game['gamePk']
                logging.info(f'Querying data for game ID {game_id}')
                game_boxscore = self.api.boxscore(game_id)
                game_data = []

                for side in game_boxscore['teams']: # either 'home' or 'away'
                    team = game_boxscore['teams'][side]
                    # player data is stored as a dict where the keys are 
                    # strings - work around this
                    player_keys = list(team['players'].keys())
                    for player_key in player_keys:
                        stat_line = {}
                        player = game_boxscore['teams'][side]['players'][player_key]
                        # This try/catch allows us to parse only players with skaterStats.
                        # We are only concerned with skater stats for this exercise, and 
                        # this works because goalies + players who didn't play won't have
                        # this
                        try:
                            stats = player['stats']['skaterStats']
                        except KeyError: 
                            continue
                        stat_line['player_person_id'] = player['person']['id']
                        stat_line['player_person_currentTeam_name'] = team['team']['name']
                        stat_line['player_person_fullName'] = player['person']['fullName']
                        stat_line['player_stats_skaterStats_assists'] = stats['assists']
                        stat_line['player_stats_skaterStats_goals'] = stats['goals']
                        stat_line['side'] = side
                        game_data.append(stat_line)

                # parse game box score data into table, then string version of table
                # to save.
                game_data = pd.DataFrame.from_dict(game_data)
                game_data_str = game_data.to_string()
                
                # generate storage key from storage ID

                storage_key = StorageKey(game_id)

                # note: had to comment this code out for debugging part 2 as I couldn't parse the csvs.
                # they were saved as minio metadata files, so I created dummy data for analysis.
                # however, the output files should have retrieved the appropriate data.
                if self.storage.store_game(storage_key,game_data_str.encode('utf-8')):
                    logging.info(f'Successfully saved {storage_key.key()}')
                else:
                    logging.error(f'Could not save {storage_key.key()}')
                



                 
def main():
    import os
    import argparse
    parser = argparse.ArgumentParser(description='NHL Stats crawler')
    # I have included startDate and endDate for the sake of unit testing the crawler.
    # By default, there will be no arguments per the Dockerfile - in this case, it will
    # simply run the crawler across the entire season.
    # (Not recommended protocol admittedly, but this assumes)
    parser.add_argument('--startDate',help='start date of stat crawler, should be of form YYYY-MM-DD')
    parser.add_argument('--endDate',help='end date of stat crawler, should be of form YYYY-MM-DD')

    args = parser.parse_args()

    startDate = args.startDate
    endDate = args.endDate

    if startDate is None and endDate is None:
        # normally would use these dates to pull all data from start of season to today. 
        # However, to shorten data process, will only look at 1st week of season.

        startDate = '2021-10-12'
        # endDate = str(datetime.today().date())
        endDate = '2021-10-19'

    logging.info(f'Loading data for games from {startDate} to {endDate}.')
    dest_bucket = os.environ.get('DEST_BUCKET', 'output')
    startDate = datetime.strptime(startDate,"%Y-%m-%d")
    endDate =   datetime.strptime(endDate,"%Y-%m-%d")
    api = NHLApi()
    

    s3client = boto3.client('s3', config=Config(signature_version='s3v4'), endpoint_url=os.environ.get('S3_ENDPOINT_URL'))
    storage = Storage(dest_bucket, s3client)
    crawler = Crawler(api, storage)

    # To try to address any issues with the API connection
    # Couldn't simulate an issue with the API so here is my best attempt.
    retry_limit = 5
    retries = 0
    failed = True
    while retries < retry_limit:
        try:
            crawler.crawl(startDate, endDate)
            retries = retry_limit
            failed = False
        except requests.exceptions.ConnectionError:
            retries += 1
            logging.error(f'Error {retries}: Connection failed. Retrying in 5 seconds.')
            time.sleep(5)
        except requests.exceptions.Timeout:
            retries += 1
            logging.error(f'Error {retries}: Connection timed out. Retrying in 5 seconds.')
            time.sleep(5)
        except requests.exceptions.HTTPError:
            retries += 1
            logging.error(f'Error {retries}: HTTP error. Retrying in 5 seconds.')
            time.sleep(5)
        except requests.exceptions.RequestException:   
            retries += 1
            logging.error(f'Error {retries}: Unspecified error with request. Retrying in 5 seconds.')
            time.sleep(5)
    if retries == retry_limit and failed:
        logging.error(f'API query failed after {retry_limit} attempts.')
    elif not failed:
        logging.info(f'Successful crawl finished {str(datetime.utcnow())}.')


if __name__ == '__main__':
    main()
