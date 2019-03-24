import requests, re, html
from logger import logger
from ns_db.postgres import Postgres
from time import sleep
from nintendo.nintendo import Nintendo

class JP_Nintendo(Nintendo):
    def __init__(self):
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
            game_id = game.get('id')
            title = self._get_game_title(game)
            game_code = game.get('icode').strip()
            category = None
            nsuid = game.get('nsuid')
            number_of_players = 0
            image_url = self._get_image_url(game)
            release_date = game.get('sdate')
            data = {
                'game_id': game_id,
                'title': title,
                'region': self._region,
                'game_code': game_code,
                'category': category,
                'nsuid': nsuid,
                'number_of_players': number_of_players,
                'image_url': image_url,
                'release_date': release_date
            }
            
            if self._game_info_exist(game_id):
                self._update_game_info(data)
            elif nsuid:
                self._create_game_info(data)
        logger.info(f'{self._region} GAMES INFO SAVED')

    def _get_game_title(self, game):
        title = game.get('title')
        return html.unescape(title)
    
    def _get_image_url(self, game):
        path = game.get('iurl')
        image_url = f'{self._base_image_url}/{path}.jpg'
        return image_url