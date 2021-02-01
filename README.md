# Hearst Reddit Challenge
A tool that uses a CSV file with `subredddit` and `count` to create a database. Pulls the `count` of top posts of all time from `subreddit` and 
stores all the information returned in a PostgreSQL database. This tool also downloads each top post's thumbnail image and save it in a directory corresponding to 
the subreddit, cropping them to 100x100 pixels

# How to Use?
Create an instance of the `Builder` class. Run `Builder.process_csv(*path*)`, where *path* is the path to the `subreddit`, `count` CSV file.

# Important Note:
This tool can only be used to create a database. It is not designed to insert values into an existing database.
