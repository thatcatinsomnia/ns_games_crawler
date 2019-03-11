import psycopg2, configparser
from logger import logger


class Postgres:
    """For handle database, the class is singleton"""
    _instance = None

    config = configparser.ConfigParser()
    config.read('config/database.ini')
    _dbname = config.get('database', 'dbname')
    _user = config.get('database', 'user')
    _password = config.get('database', 'password')

    def __new__(cls): 
        if cls._instance is None:
            cls._instance = object.__new__(cls)
            logger.info('Database instance created')
            
            try:
                connection = Postgres._instance.connection = psycopg2.connect(
                    dbname=cls._dbname, 
                    user=cls._user,
                    password=cls._password
                )
                cursor = Postgres._instance.cursor = connection.cursor()
            except Exception as error:
                logger.debug(f'Error: connection not establised {error}')
                Postgres._instance = None
            else:
                logger.info(f'connect to database...')

        return cls._instance

    def __init__(self):
        self.connection = self._instance.connection
        self.cursor = self._instance.cursor

    def query_data(self, query, data):
        try:
            self.cursor.execute(query, data)    
        except Exception as error:
            logger.info(f'ERROR query, error: {error}'.upper())
        else:
            result = self.cursor.fetchone()
            return result

    def insert_data(self, query, data):
        try:
            self.cursor.execute(query, data)
        except Exception as error:
            self.connection.rollback()
            rollback = data
            logger.debug(f'{data} ROLLBACK!!!')
            logger.debug(f'ERROR inserting data, error: {error}'.upper())
        else:
            self.connection.commit()

    def update_data(self, query, data):
        try:
            self.cursor.execute(query, data)
        except Exception as error:
            self.connection.rollback()
            rollback = data
            logger.debug(f'{data} ROLLBACK!!!')
            logger.debug(f'ERROR update data, error: {error}'.upper())
        else:
            self.connection.commit()

    def __del__(self):
        self.connection.close()
        self.cursor.close()

    
