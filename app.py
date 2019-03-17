from crawler.game_crawler import Game_Crawler
from crawler.price_crawler import Price_Crawler
from ns_db.postgres import Postgres
from logger import logger
import json, configparser, re
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
    save_counter = 0
    update_counter = 0
    for game in us_games_datas:
        game_id = game.get('id')
        title = game.get('title')
        game_code = get_us_game_code(game)
        category = get_us_game_category(game)
        nsuid = game.get('nsuid')
        number_of_players = get_us_number_of_players(game)
        image_url = game.get('front_box_art')
        game_data = (game_id, title, game_code, category, nsuid, number_of_players, image_url)

        if not nsuid:
            logger.info(f'US game {game_id} has no nsuid ==> SKIP')
            continue
        elif is_exist_in_us_database(nsuid):
            logger.info(f'US game {game_id} already exists ==> UPDATE')
            update_us_games_datas(game_data)
            update_counter += 1
        else:
            logger.info(f'Try to saved game {game_id} data...' )
            save_counter += 1
            query = f'''
            INSERT INTO us_games (id, title, game_code, category, nsuid,
            number_of_players, image_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            '''
            postgres.insert_data(query, game_data)
    logger.info(f'Updated {update_counter} US game datas')
    logger.info(f'Saved {save_counter} US game datas')

def get_us_game_code(game):
    game_code = game.get('game_code')[-5:]
    return game_code

def get_us_game_category(game):
    category = game.get('categories').get('category')
    if isinstance(category, str):
        category = [category]
    return category

def get_us_number_of_players(game):
    number_of_string = game.get('number_of_players')
    match = re.search(r'\d+', number_of_string)
    number_of_players = '0'
    if match:
        number_of_players = match.group()
    return number_of_players

def is_exist_in_us_database(nsuid):
    ''' check if the game info exist in database '''
    postgres = Postgres()
    query = 'SELECT nsuid FROM us_games WHERE nsuid = %s'
    data = (nsuid,)
    result = postgres.query_data(query, data)
    if result:
        return True
    return False

def update_us_games_datas(game_data):
    query = f'''
    UPDATE us_games SET (id, title, game_code, category, nsuid,
            number_of_players, image_url)
        = (%s, %s, %s, %s, %s, %s, %s) WHERE id = '{game_data[0]}'
    '''
    postgres = Postgres()
    postgres.update_data(query, game_data)

def get_us_games_nsuids():
    ''' Get Nintendo US games nsuid '''
    postgres = Postgres()
    query = 'SELECT nsuid FROM us_games'
    data = ()
    result = postgres.query_datas(query, data)
    logger.info(f'Get {len(result)} US games nsuid from database')
    return result

def scrape_games_price_of_country(nsuids, country):
        logger.info(f'Start to scrape {country} games price...')
        games_price_of_country = scrape_games_price_with_id(nsuids, country)
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
        logger.info(f'Request {country} games price datas from index {start_index} to {end_index}')
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
    if is_game_price_data_exist(game, area):
        logger.info(f'Trying to update nsuid: {title_id} {currency} price information')
        query = f'''
        UPDATE {area}_eshop SET (title_id, currency, sales_status, amount)
        = (%s, %s, %s, %s)
        WHERE title_id = %s AND currency = %s
        '''
        data = (title_id, currency, sales_status, amount, title_id, currency)
        postgres.update_data(query, data)
    else:
        logger.info(f'Trying to create nsuid: {title_id} {currency} price information')
        query = f'INSERT INTO {area}_eshop VALUES (%s, %s, %s, %s)'
        data = (title_id, currency, sales_status, amount)
        postgres.insert_data(query, data)

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
        logger.info(f'nsuid: {title_id} {currency} discount history already exists ==> SKIP')
    else:
        query = f'INSERT INTO {area}_discount_history VALUES (%s, %s, %s, %s, %s, %s)'
        data = (title_id, currency, discount_amount, start_datetime, end_datetime, lowest)
        postgres = Postgres()
        postgres.insert_data(query, data)
        logger.info(f'nsuid: {title_id} {currency} discount history saved')

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
    discount_amount = game.get('discount_amount')
    data = (title_id, currency)
    postgres = Postgres()
    result = postgres.query_data(query, data)

    if not result:
        return True
    history_lowest = result[0]
    current_discount_amount = Decimal(discount_amount)

    if current_discount_amount < history_lowest:
        logger.info(f'nsuid {title_id} new lowest price {current_discount_amount}')
        return True
    return False

def scrape_eu_games_datas():
    ''' Scrap all games info in Nintendo EU eshop '''
    crawler = Game_Crawler(EU_GAMES_URL)
    eu_games_datas = crawler.scrape_eu_games_datas()
    logger.info(f'Scrape {len(eu_games_datas)} eu games ...')
    return eu_games_datas

def save_eu_games_datas(eu_games_datas):
    ''' Save EU games info to eu database '''
    postgres = Postgres()
    save_counter = 0
    update_counter = 0

    for game in eu_games_datas:
        game_id = game.get('fs_id')
        title = game.get('title')
        game_code = get_eu_game_code(game)
        nsuid = get_eu_game_nsuid(game)
        number_of_players = game.get('players_to')
        category = game.get('pretty_game_categories_txt')
        image_url = game.get('image_url')

        game_data = (game_id, title, game_code, category, nsuid, number_of_players, image_url)
        if not nsuid:
            logger.info(f'EU game {game_id} has no nsuid ==> SKIP')
        elif is_exist_in_eu_database(nsuid):
            logger.info(f'EU game {game_id} already in database ==> UPDATE')
            update_eu_games_datas(game_data)
            update_counter += 1
        else:
            logger.info(f'Try to saved game {game_id} data...' )
            save_counter += 1
            query = f'''
            INSERT INTO eu_games (id, title, game_code, category,
             nsuid, number_of_players, image_url) 
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            '''  
            postgres.insert_data(query, game_data)
    logger.info(f'Saved {update_counter} EU game datas')
    logger.info(f'Saved {save_counter} EU game datas')

def get_eu_game_code(game):
    if game.get('product_code_txt'):
        game_code = game.get('product_code_txt')[0]
    else:
        game_code = ''
    game_code = game_code.strip()[-5:]
    return game_code

def get_eu_game_nsuid(game):
    if game.get('nsuid_txt'):
        nsuid = game.get('nsuid_txt')[0]
    else:
        nsuid = ''
    return nsuid

def is_exist_in_eu_database(nsuid):
    query = 'SELECT nsuid FROM eu_games WHERE nsuid = %s'
    data = (nsuid,)
    postgres = Postgres()
    result = postgres.query_data(query, data)
    if result:
        return True
    return False

def update_eu_games_datas(game_data):
    query = f'''
    UPDATE eu_games SET (id, title, game_code, category, nsuid,
            number_of_players, image_url)
        = (%s, %s, %s, %s, %s, %s, %s) WHERE id = '{game_data[0]}'
    '''
    postgres = Postgres()
    postgres.update_data(query, game_data)

def get_eu_games_nsuids():
    postgres = Postgres()
    query = 'SELECT nsuid FROM eu_games'
    data = ()
    result = postgres.query_datas(query, data)
    logger.info(f'Get {len(result)} EU games nsuid from database')
    return result

def scrape_asia_games_datas():
    crawler = Game_Crawler(ASIA_GAMES_URL)
    asia_games_datas = crawler.scrape_asia_games_datas()
    return asia_games_datas

def save_asia_games_datas(asia_games_datas):
    image_base_url = 'https://img-eshop.cdn.nintendo.net/i'
    postgres = Postgres()
    save_counter = 0
    update_counter = 0

    for game in asia_games_datas:
        game_id = game.get('id')
        title = game.get('title')
        game_code = game.get('icode')
        nsuid = game.get('nsuid')
        image_url = f'{image_base_url}/{game.get("iurl")}.jpg'
        game_data = (game_id, title, game_code, nsuid, image_url)

        if not nsuid:
            logger.info(f'ASIA game {game_id} has no nsuid ==> SKIP')
        elif is_exist_in_asia_database(nsuid):
            logger.info(f'ASIA game {game_id} already in database ==> UPDATE')
            update_asia_game_data(game_data)
            update_counter += 1
        else:
            logger.info(f'Try to saved game {game_id} data...' )
            save_counter += 1
            query = f'''
            INSERT INTO asia_games (id, title, game_code, nsuid, image_url) 
            VALUES (%s, %s, %s, %s, %s);
            '''
            postgres.insert_data(query, game_data)
    logger.info(f'Saved {update_counter} ASIA game datas')
    logger.info(f'Saved {save_counter} ASIA game datas')

def is_exist_in_asia_database(nsuid):
    query = 'SELECT nsuid FROM asia_games WHERE nsuid = %s'
    data = (nsuid,)
    postgres = Postgres()
    result = postgres.query_data(query, data)
    if result:
        return True
    return False

def update_asia_game_data(game_data):
    query = f'''
    UPDATE asia_games SET (id, title, game_code, nsuid, image_url)
     = (%s, %s, %s, %s, %s) WHERE id = '{game_data[0]}'
    '''
    postgres = Postgres()
    postgres.update_data(query, game_data)

def get_asia_games_nsuids():
    query = 'SELECT nsuid FROM asia_games'
    data = ()
    postgres = Postgres()
    result = postgres.query_datas(query, data)
    logger.info(f'Get {len(result)} ASIA games nsuid from database')
    return result

def scrape_us_games_and_save_price_datas():
    # Start to scrape US games datas
    us_games_datas = scrape_us_games_datas()
    
    # Save US games info
    save_us_games_datas(us_games_datas)

    # Get games nsuid form us countries in order to query price data
    us_nsuids = get_us_games_nsuids()

    # Scrape all games price datas in US countries
    for country in US_COUNTRIES.split(','):
        games_price_of_country = scrape_games_price_of_country(us_nsuids, country)
        save_games_price_and_discount_history_datas(games_price_of_country, country)

def scrape_eu_games_and_save_price_datas():
    # Start to scrape EU games datas
    eu_games_datas = scrape_eu_games_datas()

    # Save all EU games datas
    save_eu_games_datas(eu_games_datas)

    # # Get all nsuids from EU
    eu_nsuids = get_eu_games_nsuids()

    # Scrape all games price datas in EU countries
    for country in EU_COUNTRIES.split(','):
        games_price_of_country = scrape_games_price_of_country(eu_nsuids, country)
        save_games_price_and_discount_history_datas(games_price_of_country, country)

def scrape_asia_games_and_save_price_datas():
    # Start to scrape Asia games datas
    asia_games_datas = scrape_asia_games_datas()

    # # Save all Asia games datas
    save_asia_games_datas(asia_games_datas)

    # Get all Asia games datas
    asia_nsuids = get_asia_games_nsuids()

    # Scrape all games price datas in EU countries
    for country in ASIA_COUNTRIES.split(','):
        games_price_of_country = scrape_games_price_of_country(asia_nsuids, country)
        save_games_price_and_discount_history_datas(games_price_of_country, country)

def main():
    scrape_us_games_and_save_price_datas()
    scrape_eu_games_and_save_price_datas()
    scrape_asia_games_and_save_price_datas()

# Read settings
config = configparser.ConfigParser()
config.read('config/config.ini')
US_GAMES_URL = config.get('urls', 'US')
EU_GAMES_URL = config.get('urls', 'EU')
ASIA_GAMES_URL = config.get('urls', 'ASIA')
PRICE_URL = config.get('urls', 'ESHOP_PRICE')
US_COUNTRIES = config.get('country_code', 'US')
EU_COUNTRIES = config.get('country_code', 'EU')
ASIA_COUNTRIES = config.get('country_code', 'ASIA')

if __name__ == "__main__":
    main()