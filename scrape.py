import logging
import os
import time
import shutil
import json
from threading import Thread
import danlib
from wrapt_timeout_decorator import *

BATCH_SIZE = 2
OUTPUT_DIR = "shutter"
os.makedirs(OUTPUT_DIR, exist_ok=True)
JSON_PATH = "/workspace/scrappers/shutterstock/video_list.json"
DELAY = 5
MAX_RETRIES = 2
TIMEOUT_LEN = 30
SPLIT_EVERY = 100  #split every x batches (results will be less because batch size is variable with how many threads are busy)
COUNTER_LOGGING_STEPS = 100

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()
logger.addHandler(logging.FileHandler('logfile.log', 'a'))

proxies = {
    "http": "<redacted>",
    "https": "<redacted>"
}
dlib = danlib.Danbooru(proxies)
bucket_num = -1
counter = SPLIT_EVERY

video_list = json.load(open(JSON_PATH, "r"))

def main():
    global bucket_num
    global counter
    thread_list = []
    list_index = 0
    
    while True:
        if counter >= SPLIT_EVERY:
            counter = 0
            bucket_num += 1
            os.makedirs('scrape/' + str(bucket_num), exist_ok=True)
            logger.info('Entering bucket ' + str(bucket_num))
        counter += 1
        if counter % COUNTER_LOGGING_STEPS:
            logger.info('Counter value: ' + str(counter))

        thread_list = [t for t in thread_list if t.is_alive()]
        cur_batch = BATCH_SIZE - len(thread_list)
        print(cur_batch, 'THREADS AVAILABLE')
        for i in range(0, cur_batch):
            thread_list.append(Thread(target=scrape_post_timeout, args=(video_list[list_index + i],)))
        for thread in thread_list:
            if not thread.is_alive():
                try:
                    thread.start()
                except Exception as e:
                    print(e)
        list_index += cur_batch
        time.sleep(DELAY)


def scrape_post_timeout(videometa, retry_n=0):
    try:
        scrape_post(videometa)
    except Exception as e:
        if retry_n < MAX_RETRIES:
            scrape_post_timeout(videometa, retry_n=retry_n + 1)
        else:
            id = videometa['id']
            logger.info(f'Video with ID {id} failed after {MAX_RETRIES} tries')

@timeout(TIMEOUT_LEN)
def scrape_post(videometa):
    try:
        id = videometa['id']
        base_path = os.path.join(OUTPUT_DIR, id)
        stream, ext = dlib.get_post(videometa)
        shutil.copyfileobj(stream, open(base_path + ext, 'wb'))
        open(base_path + '.txt', 'w').write(json.dumps(videometa['description']))
        logger.info(f'Downloaded {id}!')
    except Exception as e:
        logger.info(e)


main()
