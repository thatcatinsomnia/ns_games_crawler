from crawler.game_crawler import Game_Crawler
from crawler.price_crawler import Price_Crawler
from ns_db.postgres import Postgres
from logger import logger
import json, configparser
from time import sleep

def scrape_us_games_datas():
    ''' Scrap all games info in Nintendo US eshop '''
    crawler = Game_Crawler(US_GAMES_URL)
    us_games_datas = crawler.scrape_us_games_datas()
    logger.info(f'Scrape {len(us_games_datas)} us games ...')
    return us_games_datas

def save_us_games_datas(us_games_datas):
    ''' save the games info to database '''
    postgres = Postgres()
    for game in us_games_datas:
        game_id = game.get('id')
        title = game.get('title')
        categories = json.dumps(game.get('categories'))
        nsuid = game.get('nsuid')
        number_of_players = game.get('number_of_players')
        front_box_art = game.get('front_box_art')
        if not nsuid:
            continue
        elif is_exist_in_us_database(nsuid):
            logger.info(f'US game {game_id} already exists ==> skip' )
            continue
        else:
            query = f'''
            INSERT INTO us_games (id, title, categories, nsuid, number_of_players, front_box_art) 
            VALUES (%s, %s, %s, %s, %s, %s);
            '''
            data = (game_id, title, categories, nsuid, number_of_players, front_box_art)
            postgres.insert_data(query, data)
            logger.info(f'US game {game_id} data saved.' )

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
    ''' get Nintendo US games nsuid '''
    postgres = Postgres()
    query = 'SELECT nsuid FROM us_games;'
    data = ()
    result = postgres.query_datas(query, data)
    logger.info(f'Get {len(result)} US games from database...')
    return result

def scrape_games_price(nsuids, country):
    '''The Nintendo price api only recept 50 ids every time'''
    games_price_of_country = []
    start_index = 0
    end_index = 50
    counter = 0
    while True:
        picked_nsuids = nsuids[start_index: end_index]
        if not picked_nsuids:
            break
        counter += 1
        price_url = get_price_url(picked_nsuids, country)
        price_crawler = Price_Crawler(price_url)
        games_price = price_crawler.scrape_eshop_price()
        games_price_of_country.extend(games_price)
        start_index += 50
        end_index += 50
        sleep(1)
    return games_price_of_country

def get_price_url(picked_nsuids, country):
    ''' Generate the url with ids and country '''
    ids = ','.join([nsuid[0] for nsuid in picked_nsuids])
    price_url = f'{PRICE_URL}&country={country}&ids={ids}'
    return price_url

def save_games_price_and_history(games_price_of_country, country):
    ''' save games price and discount history '''
    area = get_eshop_area(country)
    for game in games_price_of_country:
        save_game_price(game, area)

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

# def is_lowest_price(title_id, currency, discount_amount):
#     # Get the games id in database and
#     query = f'SELECT * FROM us_discount_history WHERE title_id = {title_id}'
#     return False

def save_game_price(game, area):
    ''' save games price infomation to database, insert if not exist, otherwise update the data '''
    postgres = Postgres()
    title_id = str(game.get('title_id'))
    currency = game.get('currency')
    sales_status = game.get('sales_status')
    amount = game.get('amount')
    discount_amount = game.get('discount_amount')
    start_datetime = game.get('start_datetime')
    end_datetime = game.get('end_datetime')

    if is_game_price_data_exists(game, area):
        query = f'''
        UPDATE {area}_eshop SET (title_id, currency, sales_status, amount,
        discount_amount, start_datetime, end_datetime)
        = (%s, %s, %s, %s, %s, %s, %s)
        WHERE title_id = %s AND currency =%s
        '''
        data = (title_id, currency, sales_status, amount,
        discount_amount, start_datetime, end_datetime, title_id, currency)
        postgres.update_data(query, data)
        logger.info(f'Update game {title_id} price info...')
    else:
        query = f'INSERT INTO {area}_eshop VALUES (%s, %s, %s, %s, %s, %s, %s)'
        data = (title_id, currency, sales_status, amount, discount_amount, start_datetime, end_datetime)
        postgres.insert_data(query, data)
        logger.info(f'Insert game {title_id} price info...')

def is_game_price_data_exists(game, area):
    ''' Check the if the game price info exist in database '''
    query = f'SELECT * FROM {area}_eshop WHERE title_id = %s AND currency = %s'
    title_id = str(game.get('title_id'))
    currency = game.get('currency')
    data = (title_id, currency)
    postgres = Postgres()
    result = postgres.query_data(query, data)
    if result:
        return True
    return False

def save_us_discount_history(history_data):
    ''' save the discount history to database , if exist just skip it '''
    postgres = Postgres()
    history_query = f'INSERT INTO us_discount_history VALUES (%s, %s, %s, %s, %s, %s)'
    postgres.insert_data(history_query, history_data)


# Read settings
config = configparser.ConfigParser()
config.read('config/config.ini')
US_GAMES_URL = config.get('urls', 'US')
PRICE_URL = config.get('urls', 'ESHOP_PRICE')
COUNTRIES = config.get('country_code', 'US')

if __name__ == "__main__":
    # Get all us games data
    us_games_datas = scrape_us_games_datas()
    
    # Save us games info
    save_us_games_datas(us_games_datas)

    # Get games nsuid form us countries in order to query price data
    nsuids = get_us_games_nsuids()

    # scrap all games price datas in US countries
    for country in COUNTRIES.split(','):
        logger.info(f'Start to scrape {country} games price...')
        games_price_of_country = scrape_games_price(nsuids, country)
        save_games_price_and_history(games_price_of_country, country)