import psycopg2


class DatabaseCreator:
    def __init__(self, user: str, password: str):
        self.conn = psycopg2.connect(
            database="postgres",
            user=user,
            password=password,
            host="127.0.0.1",
            port="5432",
        )
        print("connection established")
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
        first_query = '''CREATE DATABASE RedditDatabase''';
        try:
            self.cursor.execute(first_query)
            print("Database created.")
        except:
            print("Database already exists.")
            pass
        self.conn.close()
        self.conn = psycopg2.connect(
            database="redditdatabase",
            user=user,
            password=password,
            host="127.0.0.1",
            port="5432",
        )
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
    
x = DatabaseCreator("postgres", "ValarMorghul1$")
