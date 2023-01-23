import pandas as pd
import requests
from datetime import datetime
import traceback
import time
import csv
import json
import warnings
import praw

warnings.simplefilter(action='ignore', category=FutureWarning)

reddit_client_id = ''
reddit_client_secret = ''

do_step_1_now = False
do_step_2_now = False

# Step 1: Grabbing all posts ids

username = ""  # put the username you want to download in the quotes
subreddit = "ChatGPT"  # put the subreddit you want to download in the quotes
thread_id = ""  # put the id of the thread you want to download in the quotes, it's the first 5 to 7 character string of letters and numbers from the url, like 107xayi
# leave either one blank to download an entire user's or subreddit's history
# or fill in both to download a specific users history from a specific subreddit

# change this to one of "human", "csv" or "json"
# - human: the score, creation date, author, link and then the comment/submission body on a second line. Objects are separated by lines of dashes
# - csv:
output_format = "csv"

# default start time is the current time and default end time is all history
# you can change out the below lines to set a custom start and end date. The script works backwards, so the end date has to be before the start date
start_time = datetime.utcnow()  # datetime.strptime("10/05/2021", "%m/%d/%Y")
end_time = None  # datetime.strptime("09/25/2021", "%m/%d/%Y")

convert_to_ascii = False  # don't touch this unless you know what you're doing
convert_thread_id_to_base_ten = True  # don't touch this unless you know what you're doing

def write_csv_line(writer, obj, is_submission, i):
    output_list = []
    output_list.append(i)
    output_list.append(obj['id'])
    output_list.append(datetime.fromtimestamp(obj['created_utc']).strftime("%Y-%m-%d"))
    if is_submission:
        output_list.append(obj['title'])
    output_list.append(f"u/{obj['author']}")
    output_list.append(f"https://www.reddit.com{obj['permalink']}")
    if is_submission:
        if obj['is_self']:
            if 'selftext' in obj:
                output_list.append(obj['selftext'])
            else:
                output_list.append("")
        else:
            output_list.append(obj['url'])
    else:
        output_list.append(obj['body'])
    writer.writerow(output_list)


def write_json_line(handle, obj):
    handle.write(json.dumps(obj))
    handle.write("\n")
    
def download_from_url(filename, url_base, output_format, start_datetime, end_datetime, is_submission, convert_to_ascii):
    print(f"Saving to {filename}")

    count = 0
    if output_format == "human" or output_format == "json":
        if convert_to_ascii:
            handle = open(filename, 'w', encoding='ascii')
        else:
            handle = open(filename, 'w', encoding='UTF-8')
    else:
        handle = open(filename, 'w', encoding='UTF-8', newline='')
        writer = csv.writer(handle)

    previous_epoch = int(start_datetime.timestamp())
    break_out = False
    index = 1
    writer.writerow(['index', 'id', 'day', 'title', 'author', 'url', 'gallery'])
    while True:
        new_url = url_base + str(previous_epoch)
        json_text = requests.get(new_url, headers={'User-Agent': "user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36", 'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'})
        time.sleep(3.5)  # pushshift has a rate limit, if we send requests too fast it will start returning error messages
        try:
            json_data = json_text.json()
        except json.decoder.JSONDecodeError:
            time.sleep(2)
            continue

        if 'data' not in json_data:
            break
        objects = json_data['data']
        if len(objects) == 0:
            break

        for object in objects:
            previous_epoch = object['created_utc'] - 1
            if end_datetime is not None and datetime.utcfromtimestamp(previous_epoch) < end_datetime:
                break_out = True
                break
            count += 1
            try:
                if output_format == "csv":
                    write_csv_line(writer, object, is_submission, index)
                    index += 1
                elif output_format == "json":
                    write_json_line(handle, object)
            except Exception as err:
                print(f"Couldn't print object: https://www.reddit.com{object['permalink']}")
                print(traceback.format_exc())

        if break_out:
            break

        print(f"Saved {count} through {datetime.fromtimestamp(previous_epoch).strftime('%Y-%m-%d')}")

    print(f"Saved {count}")
    handle.close()

if do_step_1_now:
    filter_string = None

    filters = []
    if username:
        filters.append(f"author={username}")
    if subreddit:
        filters.append(f"subreddit={subreddit}")
    if thread_id:
        if convert_thread_id_to_base_ten:
            filters.append(f"link_id={int(thread_id, 36)}")
        else:
            filters.append(f"link_id=t3_{thread_id}")
    filter_string = '&'.join(filters)

    url_template = "https://api.pushshift.io/reddit/{}/search?limit=1000&order=desc&{}&before="

    if not thread_id:
        download_from_url("data/posts_ids.csv", url_template.format("submission", filter_string), output_format, start_time, end_time, True, convert_to_ascii)

# Step 2: Get score and comments count

if do_step_2_now:
    reddit = praw.Reddit(client_id=reddit_client_id, client_secret=reddit_client_secret, user_agent='dataset01')

    df = pd.read_csv('data/posts_ids.csv')
    df = df.reset_index()

    batchSize = 100
    totalRows = len(df.index)
    batchCount = round(totalRows / batchSize)
    remainingBatchRange = range(batchCount * batchSize, totalRows - 1)

    df2 = pd.DataFrame({'id': [], 'title': [], 'score': [], 'num_comments': []})

    for i in range(batchCount):
        print('Processing batch #' + str(i + 1) + ' from ' + str(batchCount))
        fullNames = ['t3_' + df.at[j, 'id'] for j in range(i * batchSize, min(totalRows, i * batchSize + batchSize) - 1)]
        batchData = reddit.info(fullnames=fullNames)
        for submission in batchData:
            rowData = {'id': submission.id, 'title': submission.title, 'score': submission.score, 'num_comments': submission.num_comments}
            df2 = df2.append(rowData, ignore_index=True)

    fullNames = ['t3_' + df.at[j, 'id'] for j in remainingBatchRange]
    batchData = reddit.info(fullnames=fullNames)
    for submission in batchData:
        rowData = {'id': submission.id, 'title': submission.title, 'score': submission.score, 'num_comments': submission.num_comments}
        df2 = df2.append(rowData, ignore_index=True)


    df2.to_csv('data/all_posts_additional.csv', encoding='utf-8', index=False)

# Step 3: Merge two dataframes

df = pd.read_csv('data/posts_ids.csv')
df1 = df.drop('level_0', errors='ignore', axis=1).drop('index', errors='ignore', axis=1)

df2 = pd.read_csv('data/all_posts_additional.csv')
df2 = df2.drop('title', axis=1, errors='ignore')

df_merge = pd.merge(df1, df2, on="id")
df_merge.sort_values('score', ascending=False, inplace=True)

df_merge.to_csv('data/final.csv', encoding='utf-8', index=False)
