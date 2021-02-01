import pandas as pd, praw, requests, os, psycopg2
from PIL import Image
from collections import defaultdict
from sqlalchemy import create_engine
from tables import *


class Builder:
    def __init__(self, user: str, password: str):
        self.reddit = praw.Reddit(
            client_id="9retr8Vic7M5LA",
            client_secret="KkiSZ4mezYdkSEMkO9s1S12-7mN8jw",
            user_agent="Mysterio",
        )
        self.user = user
        self.password = password

    def process_csv(self, file_path: str) -> None:
        df = pd.read_csv(file_path, header=None, names=["sub", "count"])
        for _, row in df.iterrows():
            # Validate the Subreddit and make sure it exists. Otherwise go on to the next row:
            sub, count = row
            subreddit = self.reddit.subreddit(sub)
            if not self.validate_subreddit(subreddit):
                continue
            # Create directory to place thhumbanails
            directory_path = os.path.join(os.getcwd(), sub)
            if not os.path.isdir(directory_path):
                os.mkdir(directory_path)
            # Extract thumbnails from submissions
            self.extract_thumbnails(subreddit, count, directory_path)
        # Build a dataframe object for database processing
        all_data = self.build_df(df)
        self.create_db()
        self.create_tables(all_data)


    def validate_subreddit(self, sub) -> bool:
        url = "https://www.reddit.com/" + sub._path
        response = requests.get(url)
        code = response.status_code
        if code < 200 or code > 299 and code != 502:
            print(
                sub._path,
                " is not a valid subreddit and will not be included in the database.",
            )
            return False
        return True

    def build_df(self, dataframe) -> None:
        def is_valid_type(value) -> bool:
            if type(value) in [str, int, bool, float]:
                return True
            return False

        all_keys = set()
        for _, row in dataframe.iterrows():
            sub, count = row
            subreddit = self.reddit.subreddit(sub)
            if not self.validate_subreddit(subreddit):
                continue

            top_submissions = subreddit.top("all", limit=count)

            for submission in top_submissions:
                dictionary = submission.__dict__
                for k, v in dictionary.items():
                    if v is None or is_valid_type(v):
                        if k not in all_keys:
                            all_keys.add(k)
        
        new_dictionary = defaultdict(list)

        for _, row in dataframe.iterrows():
            sub, count = row
            subreddit = self.reddit.subreddit(sub)
            if not self.validate_subreddit(subreddit):
                continue
            top_submissions = subreddit.top("all", limit=count)
            for submission in top_submissions:
                dictionary = submission.__dict__
                for key in all_keys:
                    if key not in dictionary.keys():
                        new_dictionary[key].append(None)
                    else:
                        new_dictionary[key].append(dictionary[key])

        df = pd.DataFrame.from_dict(new_dictionary)
        df.index = df["id"]
        return df

    def extract_thumbnails(self, subreddit, count: int, path: str) -> None:
        top_all_time = subreddit.top("all", limit=count)
        for submission in top_all_time:
            try:
                img = Image.open(requests.get(submission.thumbnail, stream=True).raw)
                img_resized = img.resize((100, 100))
                img_resized.save(os.path.join(path, submission.name), "PNG")
            except:
                print(
                    "Submission with ID: ",
                    submission.name,
                    " from r/"
                    + subreddit.display_name
                    + " does not have a thumbnail image.",
                )
    def create_db(self) -> None:
        conn = psycopg2.connect(
            database="postgres",
            user=self.user,
            password=self.password,
            host="127.0.0.1",
            port="5432",
        )
        print("Connection Established")
        conn.autocommit = True
        cursor = conn.cursor()
        first_query = '''CREATE DATABASE redditdatabase''';
        try:
            cursor.execute(first_query)
            print("redditdatabase created.")
        except Exception as ex:
            print(ex)
            conn.close()
        conn.close()
    
    def create_tables(self, df):
        alchemyEngine = create_engine('postgresql+psycopg2://' + self.user+ ':' + self.password + '@localhost/redditdatabase', pool_recycle=3600)
        postgreSQLConnection = alchemyEngine.connect()
        tables = [submission_table, subreddit_table, author_table, stats_table, styling_table, discipline_table, thumbnails_table, comments_table, media_table, moderator_table]
        names = ["submission", "subreddit", "author", "statistics", "styling", "discipline", "thumbnail", "comments", "media", "moderator"]

        for i in range(len(tables)):
            try:
                postgreSQLTable = names[i]
                curr = df[tables[i]]
                curr.to_sql(postgreSQLTable, postgreSQLConnection, if_exists='fail')
            except ValueError as vx:
                print(vx)
            except Exception as ex:  
                print(ex)
            else:
                print("PostgreSQL Table %s has been created successfully."%postgreSQLTable)
        postgreSQLConnection.close()


x = Builder("postgres", "ValarMorghul1$")
x.process_csv("data2.csv")
