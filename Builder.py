import pandas as pd, praw, requests, os
from PIL import Image


class Builder:
    def __init__(self):
        pass

    def process_csv(self, file_path: str) -> None:
        # Set up credentials and read CSV
        df = pd.read_csv(file_path, header=None, names=["sub", "count"])
        reddit = praw.Reddit(
            client_id="9retr8Vic7M5LA",
            client_secret="KkiSZ4mezYdkSEMkO9s1S12-7mN8jw",
            user_agent="Mysterio",
        )

        for _, row in df.iterrows():
            # Create a subreddit instance and directory for all relevant thumbnails
            sub, count = row
            directory_path = os.path.join(os.getcwd(), sub)
            if not os.path.isdir(directory_path):
                os.mkdir(directory_path)
            subreddit = reddit.subreddit(sub)
            # Iterate throught the count of top posts
            for submission in subreddit.top("all", limit=count):
                # First process the thumbnail appropriately by resizing and saving to directory
                try:
                    img = Image.open(
                        requests.get(submission.thumbnail, stream=True).raw
                    )
                    img_resized = img.resize((100, 100))
                    img_resized.save(
                        os.path.join(directory_path, submission.name), "PNG"
                    )
                except:
                    print(
                        "Submission with ID: ",
                        submission.name,
                        " from r/" + sub + " does not have a thumbnail image.",
                    )


x = Builder()
x.process_csv("data.csv")