import requests, json, uuid
from logger import logger
from ns_db.postgres import Postgres
from nintendo.nintendo import Nintendo

class EU_Nintendo(Nintendo):
    def __init__(self):
        self._url = 'https://searching.nintendo-europe.com/en/select'
        self._region = 'EU'
        self._countries = ('FR', 'CZ', 'DK', 'NO', 'PL', 'ZA', 'SE', 'CH', 'GB', 'RU', 'AU', 'NZ')

    def scrape_eu_games_info(self):
        logger.info(f'Start to scrape EU games info ...')
        rows = 9999
        eu_games = self.scrape_eu_games_info_with_rows(rows)
        return eu_games

    def scrape_eu_games_info_with_rows(self, rows):
        payload = {
            'fq': 'type:GAME AND ((playable_on_txt:\"HAC\")' \
                 'AND (dates_released_dts:[* TO NOW]) AND (nsuid_txt:*))',
            'q': '*',
            'system_type': 'nintendoswitch*',
            'sort':"score desc, date_from desc",
            'wt': 'json',
            'start': 0,
            'rows': 9999
        }

        response = requests.get(self._url, params=payload)
        response.encoding = 'utf-8'
        
        if response.status_code == 200:
            eu_games_with_rows = response.json().get('response').get('docs')
            logger.info(f'Scrape {len(eu_games_with_rows)} games from with rows: {rows}')
            return eu_games_with_rows
        else:
            logger.info(f'ERROR CODE: {response.status_code}')
        
    def save_eu_games_info(self, games):
        logger.info(f'Saving {self._region} games info...')
        for game in games:
            game_id = uuid.uuid4().hex
            title = game.get('title')
            game_code = self._get_game_code(game)
            category = self._get_game_category(game)
            nsuid = game.get('nsuid_txt')[0]
            number_of_players = game.get('players_to')
            image_url = game.get('image_url')
            release_date = game.get('dates_released_dts')[0]
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

    def _get_game_code(self, game):
        game_code = ''
        if game.get('product_code_txt'):
            game_code = game.get('product_code_txt')[0].strip()
        return game_code

    def _get_game_category(self, game):
        category = game.get('pretty_game_categories_txt')
        return category