from crawler.game_crawler import Game_Crawler
from crawler.price_crawler import Price_Crawler
from ns_db.postgres import Postgres
from logger import logger
import json, configparser
from time import sleep
from decimal import Decimal

def scrape_us_games_datas():
    ''' Scrap all games info in Nintendo US eshop '''
    crawler = Game_Crawler(US_GAMES_URL)
    us_games_datas = crawler.scrape_us_games_datas()
    logger.info(f'Scrape {len(us_games_datas)} us games ...')
    return us_games_datas

def save_us_games_datas(us_games_datas):
    ''' save the games info to database '''
    postgres = Postgres()
    counter = 0
    
    for game in us_games_datas:
        game_id = game.get('id')
        title = game.get('title')
        categories = json.dumps(game.get('categories'))
        nsuid = game.get('nsuid')
        number_of_players = game.get('number_of_players')
        front_box_art = game.get('front_box_art')
        if not nsuid:
            logger.info(f'US game {game_id} has no nsuid ==> skip' )
            continue
        elif is_exist_in_us_database(nsuid):
            logger.info(f'US game {game_id} already exists ==> skip' )
            continue
        else:
            counter += 1
            query = f'''
            INSERT INTO us_games (id, title, categories, nsuid, number_of_players, front_box_art) 
            VALUES (%s, %s, %s, %s, %s, %s);
            '''
            data = (game_id, title, categories, nsuid, number_of_players, front_box_art)
            postgres.insert_data(query, data)
            logger.info(f'Game id:{game_id} data is saved' )
    logger.info(f'Saved {counter} US game datas')

def is_exist_in_us_database(nsuid):
    ''' check if the game info exist in database '''
    postgres = Postgres()
    query = 'SELECT nsuid FROM us_games WHERE nsuid = %s'
    data = (nsuid,)
    result = postgres.query_data(query, data)
    if result:
        return True
    return False

def get_us_games_nsuids():
    ''' Get Nintendo US games nsuid '''
    postgres = Postgres()
    query = 'SELECT nsuid FROM us_games;'
    data = ()
    result = postgres.query_datas(query, data)
    logger.info(f'Get {len(result)} US games nsuid from database')
    return result

def scrape_us_games_price_of_country(us_nsuids, country):
        logger.info(f'Start to scrape {country} games price...')
        games_price_of_country = scrape_games_price_with_id(us_nsuids, country)
        logger.info(f'Scrape {len(games_price_of_country)} games price datas in {country}')
        return games_price_of_country

def scrape_games_price_with_id(nsuids, country):
    '''The Nintendo price api only recept 50 ids every time'''
    games_price_of_country = []
    start_index = 0
    end_index = 50
    counter = 0

    while True:
        picked_nsuids = nsuids[start_index: end_index]
        if not picked_nsuids:
            logger.info(f'No ids from index {start_index} to {end_index} ==> STOP')
            break
        logger.info(f'Scrape {country} games price datas from index {start_index} to {end_index}')
        counter += 1
        price_url = get_price_url(picked_nsuids, country)
        price_crawler = Price_Crawler(price_url)
        games_price = price_crawler.scrape_eshop_price()
        games_price_of_country += games_price
        start_index += 50
        end_index += 50
        sleep(1)
    return games_price_of_country

def get_price_url(picked_nsuids, country):
    ''' Generate the url with ids and country '''
    ids = ','.join([nsuid[0] for nsuid in picked_nsuids if nsuid[0]])
    price_url = f'{PRICE_URL}&country={country}&ids={ids}'
    return price_url

def save_games_price_and_discount_history_datas(games_price_of_country, country):
    ''' save games price and discount history '''
    area = get_eshop_area(country)
    logger.info(f'Start to update games price infomation in {country}...')
    postgres = Postgres()

    for game in games_price_of_country:
        save_games_price(game, area)
        if is_discount_now(game):
            save_game_discount_history(game, area)


def get_eshop_area(country):
    ''' to get the area the country belong to '''
    config = configparser.ConfigParser()
    config.read('config/config.ini')
    us = config.get('country_code', 'US')
    eu = config.get('country_code', 'EU')
    asia = config.get('country_code', 'ASIA')
    area = ''
    if country in us:
        area = 'us'
    elif country in eu:
        area = 'eu'
    elif country in asia:
        area = 'asia'
    return area

def is_discount_now(game):
    if game.get('start_datetime'):
        return True
    return False

def save_games_price(game, area):
    title_id = str(game.get('title_id'))
    currency = game.get('currency')
    sales_status = game.get('sales_status')
    amount = game.get('amount')
    postgres = Postgres()
    action_message = ''
    if is_game_price_data_exist(game, area):
        query = f'''
        UPDATE {area}_eshop SET (title_id, currency, sales_status, amount)
        = (%s, %s, %s, %s)
        WHERE title_id = %s AND currency = %s
        '''
        data = (title_id, currency, sales_status, amount, title_id, currency)
        postgres.update_data(query, data)
        action_message = 'Update'
    else:
        query = f'INSERT INTO {area}_eshop VALUES (%s, %s, %s, %s)'
        data = (title_id, currency, sales_status, amount)
        postgres.insert_data(query, data)
        action_message = 'Create'
    logger.info(f'{action_message} nsuid: {title_id} price information')
    
def is_game_price_data_exist(game, area):
    ''' Check the if the game price info exist in database '''
    query = f'SELECT title_id FROM {area}_eshop WHERE title_id = %s AND currency = %s'
    title_id = str(game.get('title_id'))
    currency = game.get('currency')
    data = (title_id, currency)
    postgres = Postgres()
    result = postgres.query_data(query, data)

    if result:
        return True
    return False

def save_game_discount_history(game, area):
    ''' save the discount history to database , if exist just skip it '''
    title_id = str(game.get('title_id'))
    currency = game.get('currency')
    discount_amount = game.get('discount_amount')
    start_datetime = game.get('start_datetime')
    end_datetime = game.get('end_datetime')
    lowest = is_lowest_price(game, area)

    if is_game_discount_history_exist(game, area):
        logger.info(f'nsuid: {title_id} discount history already exists')
    else:
        query = f'INSERT INTO {area}_discount_history VALUES (%s, %s, %s, %s, %s, %s)'
        data = (title_id, currency, discount_amount, start_datetime, end_datetime, lowest)
        postgres = Postgres()
        postgres.insert_data(query, data)
        logger.info(f'nsuid: {title_id} discount history saved')

def is_game_discount_history_exist(game, area):
    ''' query the game in discount history '''
    query = f'''
    SELECT title_id FROM {area}_discount_history 
    WHERE title_id = %s AND currency = %s AND start_datetime = %s
    '''
    title_id = str(game.get('title_id'))
    currency = game.get('currency')
    start_datetime = game.get('start_datetime')
    data = (title_id, currency, start_datetime)
    postgres = Postgres()
    result = postgres.query_data(query, data)

    if result:
        return True
    return False

def is_lowest_price(game, area):
    query = f'''
    SELECT DISTINCT ON (discount_amount) discount_amount
    FROM {area}_discount_history 
    WHERE title_id = %s AND currency = %s
    ORDER BY discount_amount ASC;
    '''
    title_id = str(game.get('title_id'))
    currency = game.get('currency')
    data = (title_id, currency)
    postgres = Postgres()
    result = postgres.query_data(query, data)

    if not result:
        return False
    history_lowest = result[0]
    current_discount_amount = Decimal(game.get('discount_amount'))

    if current_discount_amount < history_lowest:
        logger.info(f'nsuid {title_id} new lowest price {current_discount_amount}')
        return True
    return False

# Read settings
config = configparser.ConfigParser()
config.read('config/config.ini')
US_GAMES_URL = config.get('urls', 'US')
PRICE_URL = config.get('urls', 'ESHOP_PRICE')
US_COUNTRIES = config.get('country_code', 'US')

if __name__ == "__main__":
    # Get all us games data
    us_games_datas = scrape_us_games_datas()
    
    # Save us games info
    save_us_games_datas(us_games_datas)

    # Get games nsuid form us countries in order to query price data
    us_nsuids = get_us_games_nsuids()

    # Scrap all games price datas in US countries
    for country in US_COUNTRIES.split(','):
        games_price_of_country = scrape_us_games_price_of_country(us_nsuids, country)
        save_games_price_and_discount_history_datas(games_price_of_country, country)
