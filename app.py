from crawler.game_crawler import Game_Crawler
from crawler.price_crawler import Price_Crawler
from ns_db.postgres import Postgres
from logger import logger
import json, configparser
from time import sleep

def scrape_all_games_data():
    crawler = Game_Crawler(GAME_URL)
    games_data = crawler.scrape_games_data()
    logger.info(f'scrape {len(games_data)} games ...')
    return games_data

def save_games_data(games_data):
    postgres = Postgres()
    query = '''
    INSERT INTO america_games (id, title, categories, nsuid, number_of_players, front_box_art) 
    VALUES (%s, %s, %s, %s, %s, %s);
    '''

    # Save all game info to db
    for game in games_data:
        game_id = game.get('id')
        title = game.get('title')
        categories = json.dumps(game.get('categories'))
        nsuid = game.get('nsuid')
        number_of_players = game.get('number_of_players')
        front_box_art = game.get('front_box_art')
        
        # skip if game is in database
        if is_game_exist(game_id):
            logger.info(f'Skip game {game_id} exist in database.')
            continue
        else:
            data = (game_id, title, categories, nsuid, number_of_players, front_box_art)
            postgres.insert_data(query, data)
    del postgres

def is_game_exist(game_id):
    postgres = Postgres()
    query = f'SELECT id FROM america_games WHERE id=%s'
    data = (game_id,)
    result = postgres.query_data(query, data)

    if result:
        return True
    del postgres
    return False

def get_all_games_nsuid():
    postgres = Postgres()
    query = 'SELECT nsuid FROM america_games WHERE nsuid is not NULL;'
    data = ()
    result = postgres.query_data(query, data)
    del postgres
    return result

def get_price_url(picked_nsuids, country):
    ids = ','.join([nsuid[0] for nsuid in picked_nsuids])
    price_url = f'{PRICE_URL}&country={country}&ids={ids}'
    return price_url

def scrape_games_price(price_url):
    # Create price_crawler to scrape price
    crawler = Price_Crawler(price_url)
    price_datas = crawler.scrape_eshop_price()
    return price_datas

def save_eshop_price_and_history(eshop_games_datas, country):
    postgres = Postgres()
    eshop_query = f'INSERT INTO {country}_eshop VALUES (%s, %s, %s, %s, %s)'
    history_query = f'INSERT INTO {country}_discount_history VALUES (%s, %s, %s, %s)'

    for eshop_game in eshop_games_datas:
        title_id = eshop_game.get('title_id')
        amount = eshop_game.get('amount')
        discount_amount = eshop_game.get('discount_amount')
        start_datetime = eshop_game.get('start_datetime')
        end_datetime = eshop_game.get('end_datetime')

        eshop_data = (title_id, amount, discount_amount, start_datetime, end_datetime)
        history_data = (title_id, discount_amount, start_datetime, end_datetime)

        postgres.insert_data(eshop_query, eshop_data)
        postgres.insert_data(history_query, history_data)

# Read settings
urls = configparser.ConfigParser()
urls.read('config/urls.ini')
GAME_URL = urls.get('urls', 'AMERICA')
PRICE_URL = urls.get('urls', 'ESHOP_PRICE')
COUNTRIES = urls.get('country_code', 'AMERICA')

if __name__ == "__main__":
    # Get all games data
    games_data = scrape_all_games_data()
    
    # Save game info to db
    save_games_data(games_data)

    # Get game price form america countries
    nsuids = get_all_games_nsuid()

    # Get all price data in america
    for country in COUNTRIES.split(','):
        eshop_games_datas = []
        logger.info(f'Start to scrape {country} eshop games price ...')
        
        # Limit is 50, send request every 50 ids
        start_index = 0
        end_index = 50
        while True:
            picked_nsuids = nsuids[start_index: end_index]
            if picked_nsuids:
                price_url = get_price_url(picked_nsuids, country)
                start_index += 50
                end_index += 50             
            else:
                break
            scraped_datas = scrape_games_price(price_url)
            eshop_games_datas.extend(scraped_datas)
            sleep(1)
        save_eshop_price_and_history(eshop_games_datas, country)
        logger.info(f'Save {country} games price to database')