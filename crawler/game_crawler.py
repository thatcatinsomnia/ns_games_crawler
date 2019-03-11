import requests, json
from time import sleep
from logger import logger

class Game_Crawler():
    def __init__(self, url):
        self.url = url 

    def scrape_us_games_datas(self):
        us_games_datas = []
        offset = 0
        while True:
            url = f'{self.url}&offset={offset}'
            response = requests.get(url)
            if response.status_code == 200:
                respjson = json.loads(response.text.encode('UTF-8'))
                games_of_request = respjson.get('games').get('game')
                if games_of_request:
                    us_games_datas.extend(games_of_request)
                    logger.info(f'Scrape {len(games_of_request)} games from offset {offset} ...')
                    offset += 200
                    sleep(1)
                else:
                    logger.info(f'No games in offset {offset}...')
                    break
            else:
                logger.info(f'Fail with resonpse code {response.status_code} in offset {offset}')
            
        return us_games_datas