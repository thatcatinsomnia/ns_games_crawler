import requests, json
from time import sleep
from logger import logger

class Game_Crawler():
    def __init__(self, url):
        self.url = url 

    def scrape_games_data(self):
        games_data = []
        result = ''
        offset = 0
        
        while True:
            url = f'{self.url}&offset={offset}'
            response = requests.get(url)

            if response.status_code ==200:
                respjson = json.loads(response.text.encode('UTF-8'))
                games = respjson.get('games').get('game')

                if games:
                    logger.info(f'Get {len(games)} games from offset {offset} ...')
                    games_data.extend(games)
                    offset += 200
                    sleep(1)
                else:
                    logger.info(f'No game found in offset {offset}')
                    break
            else:
                logger.debug(f'CANT GET RESPONSE FROM {url}')
        
        return games_data