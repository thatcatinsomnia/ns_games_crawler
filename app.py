from logger import logger
from nintendo.na_nintendo import NA_Nintendo
from nintendo.eu_nintendo import EU_Nintendo
from nintendo.jp_nintendo import JP_Nintendo

def main():
    na_nintendo = NA_Nintendo()
    na_games = na_nintendo.scrape_na_games_info()
    na_nintendo.save_na_games_info(na_games)
    na_games_price = na_nintendo.scrape_all_games_price()
    na_nintendo.save_games_price(na_games_price)

    eu_nintendo = EU_Nintendo()
    eu_games = eu_nintendo.scrape_eu_games_info()
    eu_nintendo.save_eu_games_info(eu_games)
    eu_games_price = eu_nintendo.scrape_all_games_price()
    eu_nintendo.save_games_price(eu_games_price)

    jp_nintendo = JP_Nintendo()
    jp_games = jp_nintendo.scrape_jp_games_info()
    jp_nintendo.save_jp_games_info(jp_games)
    jp_games_price = jp_nintendo.scrape_all_games_price()
    jp_nintendo.save_games_price(jp_games_price)

if __name__ == '__main__':
    main()