import requests, re, html
from logger import logger
from database.postgres import Postgres
from time import sleep
from nintendo.nintendo import Nintendo

class JP_Nintendo(Nintendo):
    def __init__(self):
        super().__init__()
        self._url = 'https://search.nintendo.jp/nintendo_soft/search.json'
        self._base_image_url = 'https://img-eshop.cdn.nintendo.net/i'
        self._region = 'JP'
        self._countries = ('JP',)

    def scrape_jp_games_info(self):
        logger.info(f'Start to scrape JP games info...')
        jp_games = []
        page = 1
        while True:
            games_from_page = self.scrape_game_info_from_page(page)
            jp_games += games_from_page
            if len(games_from_page) < 300:
                logger.info(f'Scrape {len(jp_games)} games in JP Nintendo...')
                break
            page += 1
            sleep(1)
        return jp_games

    def scrape_game_info_from_page(self, page):
        payload = {
            'opt_sshow': 1,
            'opt_ssitu[]': ('onsale','preorder'),
            'limit': 300,
            'page': {page},
            'opt_osale': 1,
            'opt_hard':'1_HAC',
            'sort': 'sodate desc,score'
        }

        response = requests.get(self._url, params=payload)
        response.encoding = 'utf-8'

        if response.status_code == 200:
            jp_games_from_page = response.json().get('result').get('items')
            logger.info(f'Scrape {len(jp_games_from_page)} JP games from page {page}')
            return jp_games_from_page
        else:
            logger.info(f'ERROR CODE: {response.status_code}')

    def save_jp_games_info(self, games):
        for game in games:
            nsuid = game.get('nsuid')
            title = self._get_game_title(game)
            slug = None
            game_code = self._get_game_code(game)
            category = None
            number_of_players = 0
            image_url = self._get_image_url(game)
            release_date = game.get('sdate')
            data = {
                'nsuid': nsuid,
                'region': self._region,
                'title': title,
                'slug': slug,
                'description': None,
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
        match = re.search(r'&#[\d]+[\w]+;', title)
        if match:
            result = match.group(0)
            title = title.replace(result, '')
        return html.unescape(title)
        
    def _get_game_code(self, game):
        game_code = ''
        if game.get('icode'):
           game_code = game.get('icode').strip()[:-1]
        return game_code
        
    def _get_full_game_code(self, game):
        game_code = ''
        if game.get('icode'):
           game_code = game.get('icode').strip()
        return game_code

    def _get_image_url(self, game):
        path = game.get('iurl')
        image_url = f'{self._base_image_url}/{path}.jpg'
        return image_url