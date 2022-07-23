from datatable import dt, f, by, update
from transformers import pipeline
import time
from timeit import default_timer as timer
from datetime import timedelta
import re
import os
import gc

DEFAULT_VAL = -float('inf');

if not os.path.exists("D:\\factiva_cleaned\\lead_para_with_sentiment.csv"):
    lead_para = dt.fread("D:\\factiva_cleaned\\lead_para.csv")
    lead_para['sentiment'] = DEFAULT_VAL
    lead_para['confidence'] = DEFAULT_VAL
    # save to lead_para sentiment
    lead_para.to_csv("D:\\factiva_cleaned\\lead_para_with_sentiment.csv")
    del lead_para
    gc.collect();

lead_para_with_sent = dt.fread("D:\\factiva_cleaned\\lead_para_with_sentiment.csv");

sentiment_gpu = pipeline("sentiment-analysis", model = 'D:\\nlp\\transformers\\models\\distilbert-base-uncased-finetuned-sst-2-english', device = 0) # device = -1 for CPU



# lead_para_with_sent.nrows
ts = timer();
# the below executes roughly 100 iterations per second w/o sleep. With the below sleep schedule it executes roughly 22 articles per second.
# With this speed, it should take 7-8 days to process 13.5 Mn documents.
cnt = 0;
# set 0 for no sleep (observe GPU temp closely). Set 1 for default. Set <1 for aggressive sleep and >1 for more sleep.
SLEEP_MULTIPLIER = 1;
for i in range(lead_para_with_sent.nrows):
    # check if already processed
    if (lead_para_with_sent[i, 'sentiment'] != DEFAULT_VAL) and (lead_para_with_sent[i, 'confidence'] != DEFAULT_VAL):
        continue

    # increase count
    cnt = cnt + 1;

    # remove url from text
    txt = re.sub(r'http\S+', '', lead_para_with_sent[i, 'LP'])

    # sleep schedule (to avoid over-heating of GPU during sustained use!)
    # mandatory sleep for each cycle
    time.sleep(0.005 * SLEEP_MULTIPLIER);
    # conditional sleep. The nesting of if conditions is deliberate: the nested conditions are only True when all of it's parent conditions are True. This helps reduce number of comparisions.
    # With the below structure, the average number of comparisions will be 1.01111 per iteration. Without nesting, average comparisions per iteration will be 5!
    if cnt % 10**2 == 0:
        print('i:', i+1, 'cnt:', cnt);
        time.sleep(0.5 * SLEEP_MULTIPLIER)
        if cnt % 10**3 == 0:
            print(round(100*(i / lead_para_with_sent.nrows), 2), '% of work is complete')
            print('Sleeping for', 5 * SLEEP_MULTIPLIER, 'seconds');
            time.sleep(5 * SLEEP_MULTIPLIER)
            if cnt % 10**4 == 0:
                te = timer();
                print('Time elapsed:', str(timedelta(seconds = te - ts)))
                print('Sleeping for', 30 * SLEEP_MULTIPLIER, 'seconds');
                time.sleep(30 * SLEEP_MULTIPLIER)
                if cnt % 10**5 == 0:
                    print('Sleeping for', 300 * SLEEP_MULTIPLIER, 'seconds');
                    time.sleep(300 * SLEEP_MULTIPLIER)
                    if cnt % 10**6 == 0:
                        print('Saving', i, 'rows out of', lead_para_with_sent.nrows, 'roughly (', round(100*i/lead_para_with_sent.nrows, 2),'% )');
                        lead_para_with_sent.to_csv("D:\\factiva_cleaned\\lead_para_with_sentiment.csv")
                        print('Sleeping for', 300 * SLEEP_MULTIPLIER, 'seconds');
                        time.sleep(300 * SLEEP_MULTIPLIER)

    # sentiment computation. Keep in mind that some cases will fail because of token length > 512. Catch those cases!
    try:
        s = sentiment_gpu(txt)[0];
    except RuntimeError:
        print('Sentiment computation failed for i:', i);
        continue

    # update sentiment in data table
    lead_para_with_sent[i, f.sentiment] = 1 if s['label'] == 'POSITIVE' else -1
    lead_para_with_sent[i, f.confidence] = s['score']

te = timer();


lead_para_with_sent.to_csv("D:\\factiva_cleaned\\lead_para_with_sentiment.csv")

