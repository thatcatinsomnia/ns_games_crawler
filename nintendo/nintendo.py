from ns_db.postgres import Postgres
from logger import logger
import requests
from time import sleep

class Nintendo:
    def _game_info_exist(self, game_id):
        postgres = Postgres()
        query = f'SELECT id FROM ns_games WHERE id = %s'
        data = (game_id,)
        result = postgres.query_data(query, data)
        if result:
            return True
        return False

    def _update_game_info(self, data):
        postgres = Postgres()
        query = f'''
        UPDATE ns_games SET (id, title, region, nsuid, game_code, 
        category, number_of_players, image_url, release_date) 
        = 
        (%(game_id)s, %(title)s, %(region)s, %(nsuid)s, %(game_code)s, 
        %(category)s, %(number_of_players)s, %(image_url)s, %(release_date)s)
        WHERE id = %(game_id)s
        '''
        postgres.update_data(query, data)

    def _create_game_info(self, data):
        postgres = Postgres()
        query = f'''
        INSERT INTO ns_games 
        (id, title, region, nsuid, game_code, 
        category, number_of_players, image_url, release_date)
        VALUES 
        (%(game_id)s, %(title)s, %(region)s, %(nsuid)s, %(game_code)s, 
        %(category)s, %(number_of_players)s, %(image_url)s, %(release_date)s);
        '''
        postgres.insert_data(query, data)

    def scrape_all_games_price(self):
        logger.info(f'Start to scrape {self._region} games price')
        nsuids = self.get_nsuids()
        games_price = self.scrape_games_price_with_nsuids(nsuids)
        return games_price

    def get_nsuids(self):
        query = f'SELECT nsuid FROM ns_games WHERE region = %s AND nsuid is not NULL;'
        postgres = Postgres()
        datas = (self._region,)
        result = postgres.query_datas(query, datas)
        nsuids = [r[0] for r in result]
        logger.info(f'Found {len(nsuids)} nsuids in {self._region} Nintendo')
        return nsuids

    def scrape_games_price_with_nsuids(self, nsuids):
        group_of_nsuids = self._group_nsuids(nsuids)
        price_base_url = 'https://api.ec.nintendo.com/v1/price?lang=en'
        games_price = []
        for country in self._countries:
            logger.info(f'Scraping {country} games price...')
            for nsuids in group_of_nsuids:
                nsuids_for_url = ','.join(nsuids)
                url = f'{price_base_url}&country={country}&ids={nsuids_for_url}'
                response = requests.get(url)
                prices = response.json().get('prices')
                games_price += prices
                sleep(1)
        return games_price

    def _group_nsuids(self, nsuids):
        group_of_nsuids = [nsuids[i: i+50] for i in range(0, len(nsuids), 50)]
        return group_of_nsuids

    def save_games_price(self, games_price):
        for game_price in games_price:
            title_id = str(game_price.get('title_id'))
            sales_status = game_price.get('sales_status')
            price_info = self._get_regular_price_info(game_price)
            price = price_info.get('price')
            currency = price_info.get('currency')
            discount_info = self._get_discount_info(game_price)
            discount_price = discount_info.get('discount_price')
            start_datetime = discount_info.get('start_datetime'),
            end_datetime = discount_info.get('end_datetime')
            is_discount = self._is_discount(discount_info)
            data = {
                'title_id': title_id,
                'currency': currency,
                'sales_status' : sales_status,
                'is_discount': is_discount,
                'price': price,
                'discount_price': discount_price,
                'start_datetime': start_datetime,
                'end_datetime': end_datetime
            }

            if not self._is_onsale(sales_status):
                continue
            elif self._price_info_exist(data):
                self._update_price_info(data)
            else:
                self._create_price_info(data)

            # save discount hisotry
            if is_discount:
                self._save_discount_info(data)

        logger.info(f'{self._region} PRICE INFO SAVED')

    def _is_onsale(self, sales_status):
        skip_status = ('sales_termination', 'not_found', 'unreleased')
        if sales_status in skip_status:
            return False
        return True
    
    def _get_regular_price_info(self, game_price):
        price = 0
        currency = '-'
        regular_price = game_price.get('regular_price')
        if regular_price:
            regular_price = game_price.get('regular_price')
            price = regular_price.get('raw_value')
            currency = regular_price.get('currency')
        
        regular_price_info = {
            'price': price,
            'currency': currency
        }
        return regular_price_info

    def _get_discount_info(self, game_price):
        discount_price = 0
        start_datetime = None
        end_datetime = None

        if game_price.get('discount_price'):
            discount_price = game_price.get('discount_price').get('raw_value')
            start_datetime = game_price.get('discount_price').get('start_datetime')
            end_datetime = game_price.get('discount_price').get('end_datetime')

        discount_info = {
            'discount_price': discount_price,
            'start_datetime': start_datetime,
            'end_datetime': end_datetime
        }
        return discount_info

    def _is_discount(self, discount_info):
        if discount_info.get('start_datetime'):
            return True
        return False

    def _price_info_exist(self, data):
        query = f'SELECT title_id FROM {self._region}_price WHERE title_id = %(title_id)s'
        postgres = Postgres()
        result = postgres.query_data(query, data)
        if result:
            return True
        return False
    
    def _update_price_info(self, data):
        postgres = Postgres()
        query = f'''
        UPDATE {self._region}_price SET (title_id, sales_status, {data.get('currency')}, is_discount)
        = 
        (%(title_id)s, %(sales_status)s, %(price)s, %(is_discount)s)
        WHERE title_id = %(title_id)s
        '''
        postgres.update_data(query,data)

    def _create_price_info(self, data):
        postgres = Postgres()
        query = f'''
        INSERT INTO {self._region}_price (title_id, sales_status, {data.get('currency')}, is_discount)
        VALUES 
        (%(title_id)s, %(sales_status)s, %(price)s, %(is_discount)s)
        '''
        postgres.insert_data(query,data)
   
    def _save_discount_info(self, data):
        if not self._discount_history_exist(data):
            self._create_discount_info(data)

    def _discount_history_exist(self, data):
        postgres = Postgres()
        query = f'''
        SELECT title_id FROM {self._region}_discount_history
        WHERE title_id = %(title_id)s 
        AND currency = %(currency)s
        AND start_datetime = %(start_datetime)s 
        AND end_datetime = %(end_datetime)s
        '''
        result = postgres.query_data(query, data)
        if result:
            return True
        return False

    def _create_discount_info(self, data):
        postgres = Postgres()
        query = f'''
        INSERT INTO {self._region}_discount_history
        (title_id, currency, discount_price, start_datetime, end_datetime)
        VALUES
        (%(title_id)s, %(currency)s, %(discount_price)s, %(start_datetime)s, %(end_datetime)s)
        '''
        postgres.insert_data(query, data)

