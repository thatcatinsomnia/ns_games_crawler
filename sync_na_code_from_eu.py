from database.postgres import Postgres
postgres = Postgres()



def sync_with_eu():
    print(f'START SYNC GAME_CODE FROM EU')
    for r in search_result:
        nsuid = int(r[0])
        query_nsuid = str(nsuid - 1)
        check_query = f'SELECT nsuid, title, game_code FROM ns_games WHERE nsuid = \'{query_nsuid}\' AND region = \'EU\''
        result = postgres.query_data(check_query, None)
        print(f'query = {check_query}')

        if result:
            game_code = result[2]
            title = result[1]
            update_query = f'''
            -- {title}
            UPDATE ns_games 
            SET game_code = \'{game_code}\' 
            WHERE nsuid = \'{nsuid}\' AND region = \'NA\'; '''

            print(update_query)
            postgres.update_data(update_query, None)
            print(f'NA GAME_CODE UPDATED WITH EU !!!')
        else:
            print('NO MATCH DATA ==> SKIP')

if __name__ == '__main__':
    search_query = 'SELECT nsuid from ns_games WHERE ns_games.game_code IS NULL AND region = \'NA\''
    search_result = postgres.query_datas(search_query, None)
    sync_with_eu()