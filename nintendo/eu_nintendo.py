import requests, json, html, re
from logger import logger
from database.postgres import Postgres
from nintendo.nintendo import Nintendo

class EU_Nintendo(Nintendo):
    def __init__(self):
        super().__init__()
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
            'fq': 'type:GAME AND ((playable_on_txt:"HAC")) AND sorting_title:* AND *:*',
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
            if not game.get('nsuid_txt'):
                continue
            nsuid = game.get('nsuid_txt')[0]
            title = self._get_game_title(game)
            slug = None
            game_code = self._get_game_code(game)
            category = self._get_game_category(game)
            number_of_players = game.get('players_to')
            image_url = game.get('image_url')
            release_date = game.get('dates_released_dts')[0]
            description = game.get('excerpt')
            data = {
                'nsuid': nsuid,
                'region': self._region,
                'title': title,
                'slug': slug,
                'description': description,
                'game_code': game_code,
                'category': category,
                'number_of_players': number_of_players,
                'image_url': image_url,
                'release_date': release_date
            }

            if not self._game_info_exist(nsuid):
                self._create_game_info(data)
                
        logger.info(f'{self._region} GAMES INFO SAVED')
    
    def _get_game_title(self, game):
        title = game.get('title').upper()
        title = title.replace('™', '').replace('®', '').strip()
        match = re.search(r'&#[\d]+[\w]+;', title)
        if match:
            result = match.group(0)
            title = title.replace(result, '')
        return html.unescape(title)

    def _get_game_code(self, game):
        game_code = ''
        if game.get('product_code_txt'):
            game_code = game.get('product_code_txt')[0].strip()
              
        if game_code:
            game_code = game_code[-5:-1]
        return game_code

    def _get_full_game_code(self, game):
        game_code = ''
        if game.get('product_code_txt'):
            game_code = game.get('product_code_txt')[0].strip()
        return game_code

    def _get_game_category(self, game):
        category = game.get('pretty_game_categories_txt')
        return category