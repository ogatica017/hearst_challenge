import pandas as pd, praw, requests, os, psycopg2
from PIL import Image
from collections import defaultdict
from sqlalchemy import create_engine
from tables import *


class Builder:
    def __init__(self, user: str, password: str):
        """
        :param str user: Username to access PostgreSQL capabilities
        :param str password: Password to the user that is passed in as an argument

        Initializes a global Reddit instance which will be used for making subreddit and submission objects. Sets a global user and
        password for connecting to PostgreSQL.
        """
        try:
            self.reddit = praw.Reddit(
                client_id="9retr8Vic7M5LA",
                client_secret="KkiSZ4mezYdkSEMkO9s1S12-7mN8jw",
                user_agent="Mysterio",
            )
        except Exception as e:
            print(e)
        self.user = user
        self.password = password

    def process_csv(self, file_path: str) -> None:
        """
        :param str file_path: File path of the csv file containing the subreddit and count info

        Main method of the Builder class. Saves thumbnail images of valid submissions in corresponding directories. Makes a new PostgreSQL database named redditdatabase.
        """
        df = pd.read_csv(file_path, header=None, names=["sub", "count"])
        for _, row in df.iterrows():
            sub, count = row
            subreddit = self.get_subreddit(sub)
            if not self.validate_subreddit(subreddit):
                print(
                    subreddit._path,
                    " is not a valid subreddit and will not be included in the database.",
                )
                continue
            directory_path = os.path.join(os.getcwd(), sub)
            if not os.path.isdir(directory_path):
                os.mkdir(directory_path)
            self.extract_thumbnails(subreddit, count, directory_path)
        all_data = self.build_df(df)
        self.create_db()
        self.create_tables(all_data)

    def get_subreddit(self, sub: str) -> praw.models.reddit.subreddit.Subreddit:
        """
        :param str sub: Name of the subreddit from the csv

        Fetches the subreddit object in a try and fetch block in case the Reddit API is unresponsive.

        :return praw.models.reddit.subreddit.Subreddit: Returns corresponding subreddit object.
        """
        try:
            return self.reddit.subreddit(sub)
        except Exception as e:
            print(e)

    def get_top_submissions(
        self, subreddit: praw.models.reddit.subreddit.Subreddit, count: int
    ) -> praw.models.listing.generator.ListingGenerator:
        """
        :param str subreddit: Subreddit object from which to fetch submissions

        Fetches the submission generator in a try and fetch block in case the Reddit API is unresponsive

        :return praw.models.reddit.subreddit.Subreddit: Returns corresponding subreddit object.
        """
        try:
            return subreddit.top("all", limit=count)
        except Exception as e:
            print(e)

    def validate_subreddit(self, sub: praw.models.reddit.subreddit.Subreddit) -> bool:
        """
        :param praw.models.reddit.subreddit.Subreddit sub: Subreddit object used to request submission data

        Verifies the given subreddit is valid and exists.

        :return boolean: Returns True if the subreddit is valid and False otherwise
        """
        url = "https://www.reddit.com/" + sub._path
        response = requests.get(url)
        code = response.status_code
        if code < 200 or code > 299 and code != 502:
            return False
        return True

    def build_df(self, dataframe: pd.core.frame.DataFrame) -> pd.core.frame.DataFrame:
        """
        :param pandas.core.frame.DataFrame dataframe: Dataframe object with the contents of the original csv, subreddit and count.

        Builds a dataframe object containing all the fields of each submission object. This function loops through
        the subreddits listed in the orginal csv, instantianing one at a time to prevent running out of space. The first
        loop builds a dictionary containing all the attributes of submission objects. A second loop is performed to a build dataframe
        object containing all rows and columns that will be input into the database.

        :return pandas.core.frame.DataFrame: Returns dataframe with data on all submissions, which will we be used to construct the database
        """

        def is_valid_type(value) -> bool:
            if type(value) in [str, int, bool, float]:
                return True
            return False

        all_keys = set()
        for _, row in dataframe.iterrows():
            sub, count = row
            subreddit = self.get_subreddit(sub)
            if not self.validate_subreddit(subreddit):
                continue

            top_submissions = self.get_top_submissions(subreddit, count)

            for submission in top_submissions:
                dictionary = submission.__dict__
                for k, v in dictionary.items():
                    if v is None or is_valid_type(v):
                        if k not in all_keys:
                            all_keys.add(k)

        new_dictionary = defaultdict(list)

        for _, row in dataframe.iterrows():
            sub, count = row
            subreddit = self.get_subreddit(sub)
            if not self.validate_subreddit(subreddit):
                continue
            top_submissions = self.get_top_submissions(subreddit, count)
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

    def extract_thumbnails(
        self, subreddit: praw.models.reddit.subreddit.Subreddit, count: int, path: str
    ) -> None:
        """
        :param praw.models.reddit.subreddit.Subreddit subreddit: Subreddit object from which the top posts will be fetched
        :param int count: Number of top posts to be fetched
        :param str path: Path of the directory where the thumbnails will be saved

        This method requests each submission's respective thumbnail, crops the image to 100x100 pixels and saves it to the
        directory passed in path. If a thumbnail is not found, a message will be printed out on the command line, notifying the user.
        """
        top_all_time = self.get_top_submissions(subreddit, count)
        for submission in top_all_time:
            try:
                img = Image.open(requests.get(submission.thumbnail, stream=True).raw)
                img_resized = img.resize((100, 100))
                img_resized.save(os.path.join(path, submission.name), "PNG")
            except:
                print(
                    "Submission with ID: ",
                    submission.id,
                    " from r/"
                    + subreddit.display_name
                    + " does not have a thumbnail image.",
                )

    def create_db(self) -> None:
        """
        Creates the redditdabase in PostgreSQL.
        """
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
        first_query = """CREATE DATABASE redditdatabase"""
        try:
            cursor.execute(first_query)
            print("redditdatabase created.")
        except Exception as ex:
            print(ex)
            conn.close()
        conn.close()

    def create_tables(self, df: pd.core.frame.DataFrame) -> None:
        """
        :param pandas.core.frame.DataFrame df: Dataframe object containing data from all submissions corresponding
        to the subreddits in the orginal csv

        Creates PostgreSQL tables in the redditdatabase from the dataframe object.
        """
        alchemyEngine = create_engine(
            "postgresql+psycopg2://"
            + self.user
            + ":"
            + self.password
            + "@localhost/redditdatabase",
            pool_recycle=3600,
        )
        postgreSQLConnection = alchemyEngine.connect()
        tables = [
            submission_table,
            subreddit_table,
            author_table,
            stats_table,
            styling_table,
            discipline_table,
            thumbnails_table,
            comments_table,
            media_table,
            moderator_table,
        ]
        names = [
            "submission",
            "subreddit",
            "author",
            "stats",
            "styling",
            "discipline",
            "thumbnail",
            "comments",
            "media",
            "moderator",
        ]

        for i in range(len(tables)):
            try:
                postgreSQLTable = names[i]
                curr = df[tables[i]]
                curr.to_sql(postgreSQLTable, postgreSQLConnection, if_exists="fail")
            except ValueError as vx:
                print(vx)
            except Exception as ex:
                print(ex)
            else:
                print(
                    "PostgreSQL Table %s has been created successfully."
                    % postgreSQLTable
                )
        postgreSQLConnection.close()


x = Builder("postgres", "ValarMorghul1$")
x.process_csv("data2.csv")
