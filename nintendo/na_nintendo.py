import requests, re, html, uuid
from logger import logger
from ns_db.postgres import Postgres
from nintendo.nintendo import Nintendo
from time import sleep

class NA_Nintendo(Nintendo):
    def __init__(self):
        self._url = 'https://www.nintendo.com/json/content/get/filter/game?' \
                    'system=switch&availability=now&sort=featured&direction=des&limit=200'
        self._region = 'NA'
        self._countries = ('US', 'CA', 'MX')

    def scrape_na_games_info(self):
        logger.info(f'Start to scrape NA games info ...')
        na_games = []
        offset = 0
        while True:
            games_from_offset = self.scrape_games_from_offset(offset)
            na_games += games_from_offset
            if len(games_from_offset) < 200:
                logger.info(f'Scrape {len(na_games)} games in NA Nintendo...')
                break
            offset += 200
            sleep(1)
        return na_games

    def scrape_games_from_offset(self, offset):
        payload = {
            'limit': 200,
            'offset': offset,
            'system': 'switch',
            'availability': ('now', 'prepurchase'),
            'sort': 'featured',
            'direction': 'des'
        }
        response = requests.get(self._url, params=payload)
        response.encoding = 'utf-8'

        if response.status_code == 200:
            games_from_offset = response.json().get('games').get('game')
            logger.info(f'Scrape {len(games_from_offset)} NA games from offset {offset}')
            return games_from_offset
        else:
            logger.info(f'ERROR CODE: {response.status_code}')

    def save_na_games_info(self, games):
        logger.info(f'Saving {self._region} games info...')
        for game in games:
            game_id = uuid.uuid4().hex
            title = self._get_game_title(game)
            game_code = self._get_game_code(game)
            category = self._get_game_category(game)
            nsuid = game.get('nsuid')
            number_of_players = self._get_number_of_players(game)
            image_url = game.get('front_box_art')
            release_date = game.get('release_date')
            data = {
                'game_id': game_id,
                'title': title,
                'region': self._region,
                'nsuid': nsuid,
                'game_code': game_code,
                'category': category,
                'number_of_players': number_of_players,
                'image_url': image_url,
                'release_date': release_date
            }
            
            if self._game_info_exist(game_id):
                self._update_game_info(data)
            else:
                self._create_game_info(data)
        logger.info(f'{self._region} GAMES INFO SAVED')

    def _get_game_title(self, game):
        title = game.get('title')
        return html.unescape(title)
        
    def _get_game_code(self, game):
        game_code = game.get('game_code').strip()
        return game_code

    def _get_game_category(self, game):
        category = game.get('categories').get('category')
        if isinstance(category, str):
            category = [category]
        return category

    def _get_number_of_players(self, game):
        number_of_string = game.get('number_of_players')
        match = re.search(r'\d+', number_of_string)
        number_of_players = '0'
        if match:
            number_of_players = match.group()
        return number_of_players

    