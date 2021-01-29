import pandas as pd, praw, requests, os, psycopg2
from PIL import Image
from collections import defaultdict


class Builder:
    def __init__(self):
        self.reddit = praw.Reddit(
            client_id="9retr8Vic7M5LA",
            client_secret="KkiSZ4mezYdkSEMkO9s1S12-7mN8jw",
            user_agent="Mysterio",
        )

    def process_csv(self, file_path: str) -> None:
        # Set up credentials and read CSV
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
            #Build a dataframe object for database processing
            df = self.build_df(subreddit, count)
    
    def validate_subreddit(self, sub) -> bool:
        url = "https://www.reddit.com/" + sub._path
        response = requests.get(url)
        code = response.status_code
        if  code < 200 or code > 299 and code != 502:
            print(sub._path, " is not a valid subreddit and will not be included in the database.")
            return False
        return True
        
    def build_df(self, subreddit, count):
        top_submissions = subreddit.top("all", limit=count)
        all_keys = set()

        def is_valid_type(value) -> bool:
            if value in [str, int, bool, float, None]: return True
            return False

        for submission in top_submissions:
            dictionary = submission.__dict__
            for k, v in dictionary.items():
                if v is None or is_valid_type(type(v)):
                    if k not in all_keys:
                        all_keys.add(k)

        top_submissions = subreddit.top("all", limit=count)
        new_dictionary = defaultdict(list)

        for submission in top_submissions:
            dictionary = submission.__dict__
            for key in all_keys:
                if key not in dictionary.keys():
                    new_dictionary[key].append(None)
                else:
                    new_dictionary[key].append(dictionary[key])
        df = pd.DataFrame.from_dict(new_dictionary)
        return df

    def extract_thumbnails(self, subreddit, count, path) -> None:
        top_all_time = subreddit.top("all", limit= count)
        for submission in top_all_time:
            try:
                img = Image.open(
                    requests.get(submission.thumbnail, stream=True).raw
                )
                img_resized = img.resize((100, 100))
                img_resized.save(
                    os.path.join(path, submission.name), "PNG"
                )
            except:
                print(
                    "Submission with ID: ",
                    submission.name,
                    " from r/" + subreddit.display_name + " does not have a thumbnail image.",
                )            

x = Builder()
x.process_csv("data.csv")
