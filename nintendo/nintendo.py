from database.postgres import Postgres
from logger import logger
import requests
from time import sleep
from decimal import Decimal

class Nintendo:
    def _game_info_exist(self, nsuid):
        postgres = Postgres()
        query = f'SELECT nsuid FROM ns_games WHERE nsuid = %s'
        data = (nsuid,)
        result = postgres.query_data(query, data)
        if result:
            return True
        return False
    
    def _update_game_info(self, data):
        postgres = Postgres()
        query = f'''
        UPDATE ns_games SET (nsuid, title, category, 
        number_of_players, image_url, release_date) 
        = 
        (%(nsuid)s, %(title)s, %(category)s, %(number_of_players)s, 
        %(image_url)s, %(release_date)s)
        WHERE nsuid = %(nsuid)s
        '''
        postgres.update_data(query, data)

    def _create_game_info(self, data):
        postgres = Postgres()
        query = f'''
        INSERT INTO ns_games 
        (nsuid, region, title, slug, description, category, 
        number_of_players, image_url, release_date, game_code, created)
        VALUES 
        (%(nsuid)s, %(region)s, %(title)s, %(slug)s, %(description)s, %(category)s, 
        %(number_of_players)s, %(image_url)s, %(release_date)s, %(game_code)s, CURRENT_TIMESTAMP);
        '''
        postgres.insert_data(query, data)

    def scrape_all_games_price(self):
        logger.info(f'Start to scrape {self._region} games price')
        nsuids = self.get_nsuids()
        games_price = self.scrape_games_price_with_nsuids(nsuids)
        return games_price

    def get_nsuids(self):
        query = f'SELECT nsuid FROM {self._region}_games WHERE region = %s;'
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
            nsuid = str(game_price.get('title_id'))
            sales_status = game_price.get('sales_status')
            if not self._is_onsale(sales_status):
                continue
            price_info = self._get_regular_price_info(game_price)
            price = price_info.get('price')
            currency = price_info.get('currency')
            discount_info = self._get_discount_info(game_price)
            discount_price = discount_info.get('discount_price')
            start_datetime = discount_info.get('start_datetime')
            end_datetime = discount_info.get('end_datetime')

            data = {
                'nsuid': nsuid,
                'sales_status' : sales_status,
                'currency': currency,
                'price': price,
                'discount_price': discount_price,
                'start_datetime': start_datetime,
                'end_datetime': end_datetime,
            }

            if not self._price_info_exist(data):
                self._create_price_info(data)

            # save discount hisotry
            if discount_price:
                self._save_discount_info(data)

        logger.info(f'{self._region} PRICE INFO SAVED')

    def _is_onsale(self, sales_status):
        skip_status = ('sales_termination', 'not_found')
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
            discount = game_price.get('discount_price')
            discount_price = discount.get('raw_value')
            start_datetime = discount.get('start_datetime')
            end_datetime = discount.get('end_datetime')

        discount_info = {
            'discount_price': discount_price,
            'start_datetime': start_datetime,
            'end_datetime': end_datetime
        }
        return discount_info

    def _price_info_exist(self, data):
        query = f'''
        SELECT nsuid 
        FROM {self._region}_price 
        WHERE nsuid = %(nsuid)s AND currency = %(currency)s
        '''
        postgres = Postgres()
        result = postgres.query_data(query, data)
        if result:
            return True
        return False
    
    def _update_price_info(self, data):
        postgres = Postgres()
        query = f'''
        UPDATE {self._region}_price 
        SET 
        (nsuid, sales_status, currency, price)
        = 
        (%(nsuid)s, %(sales_status)s, %(currency)s, %(price)s)
        WHERE nsuid = %(nsuid)s AND currency = %(currency)s
        '''
        postgres.update_data(query,data)

    def _create_price_info(self, data):
        postgres = Postgres()
        query = f'''
        INSERT INTO {self._region}_price 
        (nsuid, sales_status, currency, price)
        VALUES 
        (%(nsuid)s, %(sales_status)s, %(currency)s, %(price)s)
        '''
        postgres.insert_data(query,data)
   
    def _save_discount_info(self, data):
        if not self._discount_history_exist(data):
            self._create_discount_info(data)

    def _discount_history_exist(self, data):
        postgres = Postgres()
        query = f'''
        SELECT nsuid FROM {self._region}_discount_history
        WHERE nsuid = %(nsuid)s 
        AND currency = %(currency)s
        AND start_datetime = %(start_datetime)s 
        AND end_datetime = %(end_datetime)s
        '''
        result = postgres.query_data(query, data)
        if result:
            return True
        return False

    def _update_discount_info(self, data):
        postgres = Postgres()
        query = f'''
        UPDATE {self._region}_discount_history
        SET
        (nsuid, currency, discount_price, start_datetime, end_datetime)
        =
        (%(nsuid)s, %(currency)s, %(discount_price)s, %(start_datetime)s, %(end_datetime)s)
        WHERE nsuid = %(nsuid)s AND currency = %(currency)s 
        AND start_datetime = %(start_datetime)s AND end_datetime = %(end_datetime)s
        '''
        postgres.update_data(query, data)

    def _create_discount_info(self, data):
        postgres = Postgres()
        query = f'''
        INSERT INTO {self._region}_discount_history
        (nsuid, currency, discount_price, start_datetime, end_datetime)
        VALUES
        (%(nsuid)s, %(currency)s, %(discount_price)s, %(start_datetime)s, %(end_datetime)s)
        '''
        postgres.insert_data(query, data)