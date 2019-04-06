from logger import logger
from database.postgres import Postgres
import requests

currencies = ('TWD','USD', 'MXN','JPY','CAD','EUR','DKK','SEK','NZD','ZAR','AUD',
'NOK','GBP','PLN','CHF','RUB', 'CZK')

def update_currency_exchange_rate():
    postgres = Postgres()
    r = get_exchange_rate()
    timestamp = r.get('timestamp')
    source_currency = r.get('base')
    rates = r.get('rates')

    for currency in currencies:
        dest_currency = currency
        rate = rates.get(dest_currency)

        if not is_data_exist(source_currency, dest_currency, timestamp):
            query = f'''
            INSERT INTO currency (source_currency, dest_currency, rate, timestamp) VALUES
            (%s, %s, %s, %s)
            '''
            data = (source_currency, dest_currency, rate, timestamp)
            postgres.insert_data(query, data)
    logger.info(f'UPDATE USD exchange rate with timestamp: {timestamp}')

def get_exchange_rate():
    url = 'https://openexchangerates.org/api/latest.json'
    app_id = '9d8cf4c745b7406280c215d5b9fb8337'
    base = 'USD'
    payload = {
        'app_id': app_id,
        'base': base
    }
    r = requests.get(url, payload)
    return r.json()

def is_data_exist(source_currency, dest_currency, timestamp):
    query = f'''
        SELECT timestamp FROM currency 
        WHERE source_currency = %s  AND dest_currency = %s AND timestamp = %s   
    '''
    data = (source_currency, dest_currency, timestamp)
    postgres = Postgres()
    result = postgres.query_data(query, data)
    if result:
        return True
    return False

def update_price_to_usd(region):
    logger.info(f'START TO UPDATE {region.upper()} TO USD...')
    postgres = Postgres()
    region = region.lower()
    query = f'''
    UPDATE {region}_price SET price_to_usd = (
        price / (
            SELECT rate 
            FROM currency
            WHERE source_currency = 'USD' AND dest_currency = currency 
            ORDER BY timestamp DESC 
            LIMIT 1
        )
    );
    '''
    postgres.update_data(query, None)
    logger.info(f'{region.upper()} CURRENCY TO_USD UPDATED')

def update_discount_price_to_usd(region):
    logger.info(f'START TO UPDATE {region.upper()} DISCOUNT PRICE TO USD...')
    postgres = Postgres()
    region = region.lower()
    query = f'''
    UPDATE {region}_discount_history SET discount_price_to_usd = (
        discount_price / (
            SELECT rate 
            FROM currency
            WHERE source_currency = 'USD' AND dest_currency = currency 
            ORDER BY timestamp DESC 
            LIMIT 1
        )
    )
    WHERE CURRENT_TIMESTAMP BETWEEN start_datetime AND end_datetime;
    '''
    postgres.update_data(query, None)
    logger.info(f'{region.upper()} DISCOUNT CURRENCY TO_USD UPDATED')

if __name__ == '__main__':
    update_price_to_usd('na')
    update_discount_price_to_usd('na')
    
    update_price_to_usd('eu')
    update_discount_price_to_usd('eu')

    update_price_to_usd('jp')
    update_discount_price_to_usd('jp')