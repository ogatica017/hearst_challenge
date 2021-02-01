# Hearst Reddit Challenge
A tool that uses a CSV file with `subredddit` and `count` to create a database. Pulls the `count` of top posts of all time from `subreddit` and 
stores all the information returned in a PostgreSQL database. This tool also downloads each top post's thumbnail image and save it in a directory corresponding to 
the subreddit, cropping them to 100x100 pixels

# How to Use?
Create an instance of the `Builder(user, password)` class. Pass in the *user* and *password* needed to access PostgreSQL locally.

Run `Builder.process_csv(path)`, where *path* is the path to the `subreddit`, `count` CSV file.

# Important Note:
This tool can ONLY be used to create a database locally. It is not designed to insert values into an existing database.
