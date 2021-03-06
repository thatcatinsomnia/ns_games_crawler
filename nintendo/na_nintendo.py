import requests, re, html
from logger import logger
from database.postgres import Postgres
from nintendo.nintendo import Nintendo
from time import sleep
from algoliasearch import algoliasearch

class NA_Nintendo(Nintendo):
    def __init__(self):
        super().__init__()
        self._url = 'https://www.nintendo.com/json/content/get/filter/game?' \
                    'system=switch&availability=now&sort=featured&direction=des&limit=200'
        self._base_img_url = 'https://www.nintendo.com'
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

    def scrape_na_games_from_algolia(self):
        logger.info(f'Start to scrape NA games info ...')
        client = algoliasearch.Client('U3B6GR4UA3', '9a20c93440cf63cf1a7008d75f7438bf')
        index = client.init_index('noa_aem_game_en_us')
        result = index.search(
        '',
        {
            'page': 0,
            'hitsPerPage': 1000,
            'facetFilters':[
                ["availability:Available now","availability:Pre-purchase"],
                ["platform:Nintendo Switch"]
            ],
        })
        
        games = result.get('hits')
        logger.info(f'GET {len(games)} NA games info')
        return games

    def save_na_games_info(self, games):
        logger.info(f'Saving {self._region} games info...')
        for game in games:
            nsuid = game.get('nsuid')
            if not nsuid:
                continue
            title = self._get_game_title(game)
            slug = game.get('slug')
            game_code = self._get_game_code(game)
            category = self._get_game_category(game)
            number_of_players = self._get_number_of_players(game)
            image_url = game.get('front_box_art')
            release_date = game.get('release_date')
            data = {
                'nsuid': nsuid,
                'region': self._region,
                'title': title,
                'slug': slug,
                'game_code': game_code,
                'category': category,
                'number_of_players': number_of_players,
                'image_url': image_url,
                'release_date': release_date
            }

            if not self._game_info_exist(nsuid):
                self._create_game_info(data)
                
        logger.info(f'{self._region} GAMES INFO SAVED')

    def save_na_games_info_with_algolia(self, games):
        logger.info(f'Saving {self._region} games info...')
        for game in games:
            nsuid = game.get('nsuid')
            if not nsuid:
                continue
            title = self._get_game_title(game)
            slug = game.get('slug')
            description = game.get('description')
            game_code = None
            category = game.get('categories')
            number_of_players = None
            box_art = game.get('boxArt')
            image_url = f'{self._base_img_url}{box_art}'
            release_date = game.get('releaseDateMask')

            data = {
                'nsuid': nsuid,
                'region': self._region,
                'title': title,
                'description': description,
                'slug': slug,
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
        game_code = game.get('game_code').strip()
        if game_code:
            game_code = game_code[-5:-1]
        return game_code

    def _get_full_game_code(self, game):
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

    