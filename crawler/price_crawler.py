import requests, json
from logger import logger

class Price_Crawler:
    def __init__(self, url):
        self.url = url

    def scrape_eshop_price(self):
        response = requests.get(self.url)
        if response.status_code == 200:
            respjson = json.loads(response.text.encode('UTF-8'))
            country = respjson.get('country')
            prices = respjson.get('prices')
            price_datas = self.filter_price_datas(prices)
            logger.info(f'scrap {len(price_datas)} games price...')

        return price_datas
            
    def filter_price_datas(self, prices):
        price_datas = []
        for price in prices:
            title_id = price.get('title_id')
            sales_status = price.get('sales_status')
            amount = self.get_regular_amount(price)
            discount_data = self.get_discount_amount_and_datetime(price)   
            
            price_data = {
                'title_id': title_id,
                'currency': amount.get('currency'),
                'amount': amount.get('amount'),
                'discount_amount': discount_data.get('discount_amount'),
                'start_datetime': discount_data.get('discount_start_datetime'),
                'end_datetime': discount_data.get('discount_end_datetime')
            }
            price_datas.append(price_data)
        return price_datas

    def get_regular_amount(self, price):
        amount = 0
        currency = '-'
        if price.get('regular_price'):
            regular_price = price.get('regular_price')
            amount = regular_price.get('raw_value')
            currency = regular_price.get('currency')

        return {'amount': amount, 'currency': currency}

    def get_discount_amount_and_datetime(self, price):
        discount_amount = 0
        discount_start_datetime = None
        discount_end_datetime = None

        if price.get('discount_price'):
            discount_amount = price.get('discount_price').get('raw_value')
            discount_start_datetime = price.get('discount_price').get('start_datetime')
            discount_end_datetime = price.get('discount_price').get('end_datetime')

        discount_amount_and_datetime = {
            'discount_amount': discount_amount,
            'discount_start_datetime': discount_start_datetime,
            'discount_end_datetime': discount_end_datetime
        }

        return discount_amount_and_datetime