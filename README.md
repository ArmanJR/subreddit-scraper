# SubReddit Scraper
Download all posts from a subreddit

## Steps
1. Get all posts ids from [pushshift.io](https://pushshift.io)
2. Query ids from reddit api in the batches of 100
3. Merge by id

## Running
Get your own [reddit api keys](https://www.reddit.com/wiki/api/) and replace:
```python
reddit_client_id = ''
reddit_client_secret = ''
```

If you want to run steps 1 and 2, set them `True`:
```python
do_step_1_now = False
do_step_2_now = False
```

Otherwise, it will use the sample data, which is **ChatGPT** subreddit on Jan 26th.

```
python3 reddit-crawler.py
```

## Acknowledgement
The first step uses a script from [Watchful1/Sketchpad](https://github.com/Watchful1/Sketchpad/blob/master/postDownloader.py).
## Contributing
Pull requests are welcome