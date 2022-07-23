# python "C:\Users\nikhi\PycharmProjects\learnPython\greedy_with_subjects.py"
import pathlib
import time

commons_file = "C:\\Users\\nikhi\\Dropbox\\PycharmProjects\\learnPython\\commons.py";
with open(commons_file, "rb") as source_file:
    code = compile(source_file.read(), commons_file, "exec")
exec(code)
# execfile("commons.py")

# Allowed time for downloads
START_DOWNLOAD = datetime.time(0, 0, 1);
END_DOWNLOAD = datetime.time(23, 59, 59);
# START_DOWNLOAD = datetime.time(9, 0, 0);
# END_DOWNLOAD = datetime.time(22, 0, 0);

IDENTITY = 0;
# random wait between [ADD_WAIT_st, ADD_WAIT_en) seconds. Chosen uniformly!
ADD_WAIT_st = 5;
ADD_WAIT_en = 15;


def check_suitable_time(start=START_DOWNLOAD, end=END_DOWNLOAD):
    # Only download within the specified time-limits
    curr_time = datetime.datetime.now().time();
    if curr_time < start:
        raise RuntimeError
    elif curr_time > end:
        raise RuntimeError
    else:
        print('Within time! No need to sleep now!');


def wait_till_suitable_time(start=START_DOWNLOAD, end=END_DOWNLOAD):
    # Only download within the specified time-limits
    curr_time = datetime.datetime.now().time();
    if curr_time < start:
        # find the time till the start of download
        diff = datetime.timedelta(days=0, hours=start.hour, minutes=start.minute, seconds=start.second) - \
               datetime.timedelta(days=0, hours=curr_time.hour, minutes=curr_time.minute, seconds=curr_time.second);
        print('Too Early! Sleeping for', diff.seconds + 1, 'seconds');
        time.sleep(diff.seconds + 1);
    elif curr_time > end:
        # find the time till the start of next download
        diff = datetime.timedelta(days=1, hours=start.hour, minutes=start.minute, seconds=start.second) - \
               datetime.timedelta(days=0, hours=curr_time.hour, minutes=curr_time.minute, seconds=curr_time.second);
        print('Too Late! Sleeping for', diff.seconds + 1, 'seconds');
        time.sleep(diff.seconds + 1);
    else:
        print('Within time! No need to sleep now!');


# Load the final ticker-action list. This list was created based on annual downloads. If we just want the tickers then choose search_type == "entire".
f = open(selenium_dir + 'factiva_sample_firms_with_tags.csv', "r", encoding="utf8")
temp = f.readlines()
f.close()
cusip = [temp[i].strip("\n").split("_")[0] for i in range(1, len(temp))]
all_tickers = [temp[i].strip("\n").split("_")[9] for i in range(1, len(temp))]
all_conm = [temp[i].strip("\n").split("_")[11] for i in range(1, len(temp))]
factiva_tag = [temp[i].strip("\n").split("_")[15] for i in range(1, len(temp))]
factiva_sim = [temp[i].strip("\n").split("_")[17] for i in range(1, len(temp))]
factiva_sim_cos = [temp[i].strip("\n").split("_")[18] for i in range(1, len(temp))]
factiva_n_art = [int(temp[i].strip("\n").split("_")[19]) for i in range(1, len(temp))]
manual_tag = [temp[i].strip("\n").split("_")[20] for i in range(1, len(temp))]
modified_comp_name = [temp[i].strip("\n").split("_")[21] for i in range(1, len(temp))]

# straight or reverse?
f = open(network_drive + 'REVERSE', "r", encoding="utf8");
temp = f.readlines()
f.close()
temp = int(temp[0]);
if temp == 1:
    REVERSE = True;
else:
    REVERSE = False;

# Ascending iteration
iter_i = [i for i in range(0, len(all_tickers))]

# limit firms?
skip_beg = 0
skip_end = 0
skip_files_beg = [i for i in range(0, skip_beg)]
skip_files_end = [i for i in range((len(all_tickers) - skip_end), len(all_tickers))]

# Descending iteration
if REVERSE is True:
    iter_i = [len(iter_i) - 1 - i for i in range(0, len(iter_i))]


# Randomized iteration
# iter_i = [i for i in range(0, len(all_tickers))]
# shuffle(iter_i)


def restart_article_download(norun = False):
    global recheck_flag
    global driver  # driver is a global variable where multiple runs of restart_article_download() utilize the same variable

    global all_verified
    global all_completed
    global all_failed
    global all_folders
    global all_holes
    global all_lock

    if norun == False:
        # driver = open_factivia_url(bypass_maintenance_page=False, bypass_customize_homepage_page=False, factiva_showing_snapshot_page=True);
        driver = open_factivia_from_remotexs(bypass_maintenance_page=False, bypass_customize_homepage_page=False, factiva_showing_snapshot_page=True);
        factivia_set_up(driver, __date_range_type=None, __source_set=False,
                        __subject_set=False);  # Do not set any source or subject. I will set Date later

    holes_cusip = glob.glob(factivia_dir_articles + '*__.HOLES');
    # holes_cusip = [holes_cusip[i][33:41] for i in range(0, len(holes_cusip))]

    # Store the entire list of cusips for verified, completed, failed, folders and holes files. This is done to reduce search overhead in case of download restart
    # verified
    a = glob.glob(network_drive + 'csv\\' + '*');  # only csv files reside in this folder
    all_verified = [a[i].split('\\')[5][0:8] for i in range(0, len(a))]
    # everything together: completed, failed, folder, holes and lock
    a = glob.glob(factivia_dir_articles + '*');
    a = [a[i].split('\\')[5].lstrip('_') for i in range(0, len(a))];
    c = [a[i].split('__')[0] for i in range(0, len(a))];  # cusip
    b = [a[i].split('__')[-1] for i in range(0, len(a))];  # extension
    # if b[i] == c[i], then it is a folder
    for i in range(0, len(b)):
        if b[i] == c[i]:
            b[i] = '.FOLDER'
    b = [b[i].lstrip('.') for i in range(0, len(b))]
    all_completed = list()
    all_failed = list()
    all_folders = list()
    all_holes = list()
    all_lock = list()
    for i in range(0, len(b)):
        if b[i] == 'FAILED':
            all_failed.append(c[i])
        if b[i] == 'COMPLETE':
            all_completed.append(c[i])
        if b[i] == 'FOLDER':
            all_folders.append(c[i])
        if b[i] == 'HOLES':
            all_holes.append(c[i])
        if b[i] == 'LOCK':
            all_lock.append(c[i])

    # The case where all files have not been downloaded and a lock file exists can be because of either:
    # (i)  some other process is currently downloading the file
    # (ii) downloading files ended abruptly without releasing lock file
    # Case (i) is okay and the process will clear the lock upon completion. But case (ii) can't be dealt with unless we use different IDENTITY for each process.
    # A simple work-around for case (ii) is to not let a new process start a download whose lock file exists. Also all the lock files needs to be deleted periodically to counter abrupt disruptions
    # I am using the below 'Experimental' code as a solution. I also added 'continue' in the if-else clause that follows the below random deletion clause

    # Experimental Stuff: delete all the lock files that are more than 4 hours old. The idea is that it is highly unlikely that a single ticker would be downloaded for more than 4 hours
    # for f in all_lock:
    #     fname = factivia_dir_articles + '__' + f + '__.LOCK'
    #     # if seconds_since_creation(fname) > 4 * 3600:
    #     release_file(fname)

    # Loop through all tickers
    for i in iter_i:

        # limit firms
        if i in skip_files_beg:
            continue;
        if i in skip_files_end:
            continue;

        # some problem with below cusips:
        # 02376R10: american airlines groups
        # 09702310: boeing company
        # 17296742: citigroup inc
        # 24736170: delta airlines inc
        # 87264910: TRW Inc.
        if cusip[i] in ['02376R10', '09702310', '17296742', '24736170', '87264910']:
            continue

        # if cusip[i] not in holes_cusip:
        #     continue;

        check_suitable_time();

        print("i =", i, "out of", len(all_tickers), "tic =", all_tickers[i], "cusip =", cusip[i], "compustat_name =", all_conm[i], "factiva_tag =", factiva_tag[i],
              "factiva_sim_cos =", '' if factiva_sim_cos[i] in ('', '""') else f"{float(factiva_sim_cos[i]):0.2f}");

        tic = all_tickers[i];
        conm = all_conm[i];
        dir_name = factivia_dir_articles + cusip[i] + '\\';
        lock_file = factivia_dir_articles + '__' + cusip[i] + '__' + '.LOCK';
        verified_file = network_drive + 'csv\\' + cusip[i] + '.csv';
        completed_file = factivia_dir_articles + '__' + cusip[i] + '__' + '.COMPLETE';
        hole_file = factivia_dir_articles + '__' + cusip[i] + '__' + '.HOLES';
        failed_file = factivia_dir_articles + '__' + cusip[i] + '__' + '.FAILED';
        lock_taken = False
        last_file_starting_dt = last_file_ending_dt = None

        # set both to empty for each new ticker/cusip
        holes_start_list = list();
        holes_end_list = list();

        # fds extra_text: the source "Knobias" seems a faulty source and needs to be excluded from searches. This is particularly problemetic with RECHECK files
        fds_extra_text = '(wc>=0 not SN=Knobias)';

        if my_path_exists(verified_file, cusip[i], all_verified) and (norun is False):

            # Experimental:
            # As i gets larger, then each restart of the download process will have to iterate through the entire cusips before i to get
            # to the correct firm to download. This can take a lot of time since failures happen quite often during the download process
            # and the verification, deletion etc happens over a network storage which will be slow.
            # As an experimental change, I am going to continue from here without checking for completed, lock and failed files. The idea is
            # that once a file has been verified, it is unlikely to have any issues (like missing complete file or an unnecessary failed/lock
            # file. This should reduce the initial wait time!
            # continue

            # all htmls downloaded and converted to CSV without holes or errors. Nothing to be done. Move to next.
            print('All files for cusip:', cusip[i], 'and tic:', tic,
                  'have been downloaded and verified. Moving to next')

            # Make sure there is no lock file or failed file for this
            # Experimental: comment the below two
            # release_file(lock_file);
            # release_file(failed_file);

            # Also make sure there exists a completed file, if not create one
            if not my_path_exists(completed_file, cusip[i], all_completed):
                all_files = glob.glob(dir_name + "*.html")
                all_art = [int(f.replace('.html', '').replace('__RECHECK__', '').split('_')[7]) for f in all_files];
                file = open(completed_file, "w")
                file.write(str(sum(all_art)));
                file.close()

            # you may safely go to next ticker
            continue

        if my_path_exists(completed_file, cusip[i], all_completed):

            # completed files have all html downloaded but not converted and verified (done in R code: html_to_df.R)
            # For this case, we need to check for holes. If holes are present create a hole file, if not then delete an existing hole file
            # Although if a hole_file already exists then we need not create/delete it again
            # Thus, if a hole file exists, then just move on. If it doesn't exist and there are holes then create one!
            # Deleting a hole_file should be done somewhere else
            # It is fine to have a completed as well as a hole_file. It means that download has finished ...
            # ... and R code has identified and deleted html files with problems. But we still need to download the problemetic files in python

            print('All files downloaded for tic:', tic,
                  'but not verified for holes. Checking the status of holes and hole_file and moving to next')

            # remove failed_file
            # Experimental: comment the below
            # release_file(failed_file)

            if os.path.exists(hole_file):
                # hole_file exists. It must be created earlier and haven't been resolved.
                # if there are no holes then this can be safely deleted
                if check_holes(dir_name, start_date, end_date) == False:
                    release_file(hole_file, log_str='Deleted the hole_file as there are no holes. Going to next.')
                    continue
                else:
                    # hole_file exists and there are holes in the directory. Take care of these holes
                    print('hole_files exists. Working on it now.')
                    [holes_start_list, holes_end_list] = check_holes(dir_name, start_date, end_date, ret='list')
                    pass
            else:
                # release lock file only if there is no hole file
                # Experimental: comment the below
                # release_file(lock_file)
                # hole_file doesn't exists. Here we must check for existence of holes
                if check_holes(dir_name, start_date, end_date) == False:
                    # no hole_file and no holes in cusip. Good to go!
                    print('No hole_file OR holes found. Going to next.')
                    continue
                else:
                    # no hole_file but there are holes in cusip. create a hole file. Also populate the list of holes.
                    file = open(hole_file, "w")
                    file.close()
                    [holes_start_list, holes_end_list] = check_holes(dir_name, start_date, end_date, ret='list')
                    print('No hole_file but holes found. Creating the hole file and taking care of it.')
                    pass

        if my_path_exists(failed_file, cusip[i], all_failed):
            # already failed, move to next
            print('Search for tic:', tic, 'had failed earlier. Moving to next')
            # Since it is mark failed, there must be no lock_file for this
            # Experimental: comment the below
            # release_file(lock_file)
            continue

        if not my_path_exists(dir_name, cusip[i], all_folders):
            # create the folder if it doesn't exist
            print('creating dir:', cusip[i])
            os.mkdir(dir_name)
        else:
            # If the directory already exists, then before proceeding any further it makes sense to find out all the files that might be already present
            # Although this process needs to be skipped if there exists a completed_file as well as hole_file
            if my_path_exists(completed_file, cusip[i], all_completed) and os.path.exists(hole_file):
                pass
            else:
                all_files = glob.glob(dir_name + tic + "*.html");
                all_files.sort();
                if len(all_files) == 0:
                    # no files present. proceed to download
                    pass
                else:
                    # the below counts all num_art and sums them
                    all_art = [int(f.replace('.html', '').replace('__RECHECK__', '').split('_')[7]) for f in all_files];
                    # Now I am assumming that files will be downloaded in increasing order of date ranges.
                    # I will capture starting date and ending date of the last file to capture the inputs of the last search
                    # Using these I shall create the optimal period for the next search
                    last_file = max(all_files)
                    last_file_starting_dt = datetime.datetime(int(last_file.split("_")[2][0:4]),
                                                              int(last_file.split("_")[2][4:6]),
                                                              int(last_file.split("_")[2][6:8]));
                    last_file_ending_dt = datetime.datetime(int(last_file.split("_")[4][0:4]),
                                                            int(last_file.split("_")[4][4:6]),
                                                            int(last_file.split("_")[4][6:8]));
                    if last_file_ending_dt == end_date:
                        # Okay, so we came across a directory which has all its files downloaded, yet there was no COMPLETED file present. Create it!
                        # If there is a lock_file, then change it to completed_file
                        if os.path.exists(lock_file):
                            # make sure you can close a file opened by another process
                            file = open(lock_file, "w");
                            file.write(str(sum(all_art)));
                            file.close();
                            os.rename(lock_file, completed_file)
                            print('Releasing lock for cusip:', cusip[i],
                                  'as all files have been downloaded. Also marking', cusip[i], 'representing tic:', tic,
                                  'as completed')
                        else:
                            print('Marking cusip:', cusip[i], 'representing tic:', tic, 'as completed')
                            file = open(completed_file, "w")
                            file.write(str(sum(all_art)));
                            file.close()
                        # since all files have already been downloaded. Go to next cusip
                        print('All files downloaded for tic:', tic, 'Moving to next')
                        # move to next
                        continue

        # the below is experimental. I just want to loop through all firms and make sure the ones completed are correctly marked with a complete tag!
        if norun == True:
            continue;

        # see if this ticker is available for exclusive lock. Note that at this point if there are any files in dir_name, the list of files is in all_files variable (see above)
        # We would also need to take lock in case both completed_file and hole_file exist together
        if os.path.exists(lock_file):
            # Experimental stuff: continue below
            continue
            print('lock for', dir_name, 'exists')
            # lock exists. Check if the identity is same or not
            file = open(lock_file, "r")
            id = file.readlines();
            file.close()
            id = int(id[0])
            if id == IDENTITY:
                print('My identity:', IDENTITY, 'matches with saved identity:', id)
                # it must be that either there are no files in this directory OR if there are any files then we already know about those from directory check if-else
                # Nothing needs to be done here
                pass
            else:
                # identity is different. Try another cusip
                # I only opened (not created) the lock_file, hence no need to release it
                print('My identity:', IDENTITY, 'does not match with saved identity:', id)
                continue
        else:
            # Acquire lock and write current identity
            print('Acquiring lock for', cusip[i])
            file = open(lock_file, "w")
            file.write(str(IDENTITY))
            file.close()
            lock_taken = True
            # It must be that files were present in directory but a lock file wasn't.
            # No need to go through the list of files we already did that in the directory check if-else

        # the below function will return 0 if there are no matches, 1 if there is exactly one match and 2 if there are multiple (>= 2) matches.
        # A company ticker will be selected only when ret is 1
        # clear free text search box before entering ticker
        send_ticker_to_free_text_box(driver, '');
        ret = try_catch_add_company(driver, conm, ret=True);
        if ret == 1:
            # good to go!
            send_ticker_to_free_text_box(driver, fds_extra_text);
            print('Got', ret, 'results by searching company:', conm, 'for tic:', tic)
            pass
        else:
            # zero OR multiple matches. No good
            print('Got', ret, 'results by searching company:', conm, 'for tic:', tic)
            # Here before releasing the lock we can try fds tags
            # FDS tags mayn't be 100% accurate. Thus for now try the ones with sim_cos == 1
            if factiva_tag[i] is not None and factiva_tag[i] not in ('', '""') and factiva_sim_cos[i] not in ('', '""') and math.isclose(float(factiva_sim_cos[i]), 1):
                # clear the current company (if any)
                try_catch_add_company(driver, '', ret=True);
                # send the fds tag
                send_ticker_to_free_text_box(driver, 'fds=' + factiva_tag[i] + ' and ' + fds_extra_text);
            # FDS tag is manually verified. Use this!
            elif manual_tag[i] is not None and manual_tag[i] not in ('', '""'):
                # clear the current company (if any)
                try_catch_add_company(driver, '', ret=True);
                # send the fds tag
                send_ticker_to_free_text_box(driver, 'fds=' + manual_tag[i] + ' and ' + fds_extra_text);
            else:
                # Release lock
                if lock_taken:
                    release_file(lock_file, log_str='cusip: ' + cusip[i])
                # remove directory
                if os.path.exists(dir_name) and os.listdir(dir_name).__len__() == 0:
                    os.rmdir(dir_name)
                    print('folder for', cusip[i], 'deleted as it had no files')
                # create a failed file
                file = open(failed_file, "w")
                file.write(str(IDENTITY))
                file.close()
                print('Failed with tic:', tic, 'Moving to next')
                # continue to next ticker
                continue


        # select manual date entering
        date_range = driver.find_element_by_xpath('//*[@id="dr"]')
        date_range_select = Select(date_range)
        date_range_select.select_by_visible_text("Enter date range...")


        # search number of articles for the entire duration
        (num, first_date, not_needed__) = try_new_date(driver, start_date, end_date, skip_modify_search=True);
        print('Found number of articles to be', num, 'and date of first articles to be', first_date);
        # save a file with zero articles from start_date to first_date-1
        # make sure that we do not want to create a 0 articles file if there are holes or a complete file present
        if num > 100 and last_file_starting_dt is None and first_date is not None and (my_path_exists(completed_file, cusip[i], all_completed) is False) and (os.path.exists(hole_file) is False):
            filename = dir_name + "_".join(
                [tic, "from", start_date.strftime("%Y%m%d"), "to", (first_date - datetime.timedelta(1)).strftime("%Y%m%d"), "num_art",
                 f"{0:05d}"]) + ".html"
            # only download if the file doesn't exists
            print('Saving FIRST_HTML with 0 articles for tic:', tic, 'from date', start_date, 'to', (first_date - datetime.timedelta(1)));

            if not os.path.exists(filename):
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write("")
                # add file details to history file
                # comment for now
                # append_entry_in_history_file(filename, hist_cusip=cusip[i], history_file=network_drive + 'html_history.csv')

            # set last_file_starting_dt and last_file_ending_dt after saving 0 article file
            last_file_starting_dt = start_date;
            last_file_ending_dt = (first_date - datetime.timedelta(1));


        # for time being
        # if num > 100000 and (os.path.exists(hole_file) is False):
        #     print("Skipping i =", i, "out of", len(all_tickers), "tic =", all_tickers[i], "since it has more articles. This is temporary!");
        #     # click modify search
        #     modify_search(driver);
        #     # go to next ticker
        #     continue;

        # if there are no articles then store an empty string, click modify search and continue
        if num == 0:
            filename = dir_name + "_".join(
                [tic, "from", start_date.strftime("%Y%m%d"), "to", end_date.strftime("%Y%m%d"), "num_art",
                 f"{num:05d}"]) + ".html"
            # only download if the file doesn't exists
            if not os.path.exists(filename):
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write("")
                # add file details to history file
                # comment for now
                # append_entry_in_history_file(filename, hist_cusip=cusip[i], history_file=network_drive + 'html_history.csv')
            # click modify search
            modify_search(driver);
            # Delete the lock_file that was acquired.
            release_file(lock_file, log_str='cusip: ' + cusip[i])
            # go to next ticker
            continue;


        # Experimental: remaining articles to download.
        # Make sure to count already downloaded files. Also the below is not valid in case of hole files
        setting_last_date = False; # this would be set to true when we use rem_num_art info
        if os.path.exists(hole_file):
            rem_num_art = float('inf');
        else:
            all_files = glob.glob(dir_name + tic + "*.html");
            all_files.sort();
            if len(all_files) == 0:
                # no files present
                rem_num_art = num;
            else:
                # the below counts all num_art and sums them
                all_art = [int(f.replace('.html', '').replace('__RECHECK__', '').split('_')[7]) for f in all_files];
                rem_num_art = num - sum(all_art);
            setting_last_date = False;
            print('i =', i, 'Currently', rem_num_art, 'articles are remaining out of total', num, 'articles');


        # compute period
        period = int(math.pow(2, math.ceil(math.log2(
            100 * TOTAL_DAYS / num))));  # choose period in powers of 2. Note that period is higher than 100*TOTAL_DAYS/num to begin with
        period = min(period, TOTAL_DAYS);  # but it should be less than TOTAL_DAYS

        # the use of download_dur is to aid in selecting optimal window to get close to 100 articles in a search. It is set whenever there is a successful search (<= 100 articles)
        download_dur = None;
        # in the course of downloading all articles for a firm, I keep changing the begin and end dates.
        # new_start_date and new_end_date always lie within [start_date, end_date] and are used to perform a search
        new_start_date = start_date;
        new_end_date = start_date;  # start with the first date
        # If I am resuming download after an abrupt termination, then I use the dates from the last file downloaded. This can help save repeat searches
        if (last_file_starting_dt is not None) and (last_file_ending_dt is not None):
            # set new_start_date based on last saved file. new_end_date will be set later in the while loop.
            new_start_date = last_file_starting_dt;
            last_file_starting_dt = None;
        # counter is only used for printing information
        counter = 0;
        # In the last search new_end_date will equal end_date and hence the below loop will terminate
        while new_end_date < end_date:
            # In case of 0 searches or fresh search (download_dur == 0), start with the default period.
            # But if we are doing successive downloads then use download_dur to set optimal new_prd (else clause below)
            if (download_dur is None) or num_art == 0:
                new_prd = period;
            else:
                # Also make sure that you do not go beyond new_prd/2 or 2*new_prd
                min_rng = int(max(new_prd / 2, 1));
                max_rng = int(new_prd * 2);
                new_prd = round((100 / num_art) * download_dur);
                new_prd = max(new_prd, min_rng);
                new_prd = min(new_prd, max_rng);
            increament = datetime.timedelta(new_prd - 1);
            counter = counter + 1;
            # ser recheck to false before downloading a new file
            recheck_flag = False
            # the below should be false for every new html file download
            using_hundreth_date = False
            # the idea of below while loop is to find a starting and ending date range that gives <= 100 articles. When this happens we break out of the loop and download the file
            while True:

                # need to consider the special case of presence of hole files
                if os.path.exists(completed_file) and os.path.exists(hole_file) and len(holes_start_list) > 0 and len(holes_end_list) > 0:
                    # here we already have new_start and new_end dates. No need to search and optimize
                    new_start_date = holes_start_list[0];
                    new_end_date = holes_end_list[0];
                    (num_art, not_needed__, not_needed__) = try_new_date(driver, new_start_date, new_end_date, dir_search=dir_name, tic_search=tic);
                    if num_art > 100:
                        if new_end_date > new_start_date:
                            # divide in two equal parts
                            l1 = [new_start_date, None]
                            l2 = [None, new_end_date]
                            d = (new_end_date - new_start_date).days;
                            d = int(d / 2);
                            l1[1] = l1[0] + datetime.timedelta(d);
                            l2[0] = l1[1] + datetime.timedelta(1);
                            # Replace the the first entry of holes_start_list and holes_end_list with start/end entries from l1 and l2.
                            holes_start_list = [l1[0], l2[0]] + holes_start_list[1:];
                            holes_end_list = [l1[1], l2[1]] + holes_end_list[1:];
                            # start again
                            continue;
                        else:
                            print("Setting __RECHECK__ on tic:", tic, 'since it has', num_art, 'articles')
                            recheck_flag = True  # to set slightly different filename for easy identification and later redressal

                    # break from the loop to go to the download part. Before that remove first element from list
                    holes_start_list = holes_start_list[1:]
                    holes_end_list = holes_end_list[1:]
                    download_dur = None
                    break;

                new_end_date = new_start_date + increament;
                # the below shall be true when restarting download after abrupt termination
                if last_file_ending_dt is not None:
                    new_end_date = last_file_ending_dt;
                    last_file_ending_dt = None;
                if new_end_date > end_date:
                    new_end_date = end_date;
                    new_prd = (new_end_date - new_start_date).days + 1;
                    increament = datetime.timedelta(new_prd - 1);

                # If less than 100 articles are left, then set the last date as end_date
                if setting_last_date is False and rem_num_art <= 100:
                    setting_last_date = True;
                    print('i =', i, 'Currently', rem_num_art, 'articles are remaining out of total', num, 'articles. Setting new_end_date to', end_date);
                    new_end_date = end_date;
                    new_prd = (new_end_date - new_start_date).days + 1;
                    increament = datetime.timedelta(new_prd - 1);

                if num > 100:
                    # we have more than 100 articles, try a new search with new date ranges
                    (num_art, not_needed__, hundreth_date) = try_new_date(driver, new_start_date, new_end_date, dir_search=dir_name, tic_search=tic);
                else:
                    # in this case the page is already loaded and doesn't require re-loading and we can download all articles (<= 100)
                    num_art = num;

                if num_art <= 100:
                    # download articles
                    print("Success. num_art: ", num_art);
                    # set download_dur to the actual number of days used for this download. We will use this in conjunction with num_art to arrive at an optimal date range for the next file download
                    download_dur = (new_end_date - new_start_date).days + 1;
                    # In download_dur we downloaded num_art articles. Extrapolating tells that 100 articles would fit in (100/num_art)*download_dur. We can use this to make better guess for next duration.
                    break
                else:
                    # if increment is 0 then it means that there are more than 100 articles in single day
                    if increament.days <= 0:
                        print("Setting __RECHECK__ on tic:", tic, 'since it has', num_art, 'articles')
                        recheck_flag = True  # to set slightly different filename for easy identification and later redressal
                        assert new_start_date == new_end_date
                        break
                    # experimental: set new_end_date based on last available date from articles
                    if hundreth_date is not None and hundreth_date > new_start_date and using_hundreth_date is False:
                        using_hundreth_date = True;
                        new_prd = hundreth_date - new_start_date # we are guaranteed to find less than 100 articles between [new_start_date, hundreth_date-1]. Thus new_prd is (hundreth_date-1) - new_start_date + 1.
                        new_prd = new_prd.days
                        hundreth_date = None
                        print("Using hundreth_date in setting end_date for tic:", tic)
                    else:
                        # reduce the new period. Ise 0.9 times the optimal period to aid faster reduction in date range.
                        new_prd = max(int(new_prd / 2), int(math.floor(0.9 * new_prd / (num_art / 100))));  # Don't reduce period by more than one-fourth
                    # update increment
                    increament = datetime.timedelta(new_prd - 1);  # start with period

            # Download data
            # name of file to be saved
            filename = dir_name + "_".join(
                [tic, "from", new_start_date.strftime("%Y%m%d"), "to", new_end_date.strftime("%Y%m%d"), "num_art",
                 f"{num_art:05d}"]) + ".html"
            if recheck_flag == True:
                filename = filename[0:(len(filename) - 5)] + '__RECHECK__' + '.html';
            # only download if the file doesn't exists
            if not os.path.exists(filename):

                # reduce remaining articles
                rem_num_art = rem_num_art - num_art;
                using_hundreth_date = False; # for the next turn
                setting_last_date = False; # for the next turn
                print('i =', i, 'Currently', rem_num_art, 'articles are remaining out of total', num, 'articles. About to save', num_art, 'articles');

                if num_art == 0 or recheck_flag == True:
                    # save empty string in file
                    with open(filename, 'w', encoding='utf-8') as f:
                        f.write("")
                    # add file details to history file
                    # comment for now
                    # append_entry_in_history_file(filename, hist_cusip=cusip[i], history_file=network_drive + 'html_history.csv')
                else:
                    # set display options to include indexing
                    driver.find_element_by_xpath('//*[@id="ViewTab"]/a').click()
                    driver.find_element_by_xpath('//*[@id="dfdCtrl"]/li[3]').click()

                    headlines_checkbox = ['//*[@id="selectAll"]/input']
                    try_catch_find_element_by_xpath(driver, headlines_checkbox, "click", info="checkbox: ")

                    # click article format for saving (4th button). Use mouse actions
                    button = '//*[@id="listMenuRoot"]/li[4]/a'
                    download_format = '//*[@id="listMenu-id-2"]/li[2]/a'  # full articles
                    try_catch_find_element_by_xpath(driver, [button], "click", info="download articles: ")
                    time.sleep(1);
                    try_catch_find_element_by_xpath(driver, [download_format], "click", info="download articles: ")

                    # Go to next handle (i.e. 3rd one where news is displayed)
                    handles = driver.window_handles
                    driver.switch_to.window(handles[2])  # starts from 0

                    # wait for page to load
                    try_catch_and_WAIT_find_element_by_xpath(driver,
                                                             ['//*[@id="navcontainer"]/table/tbody/tr/td[1]/h1'],
                                                             ec_wait=less_wait,
                                                             fun="presence_of_element_located", info="", tries=20,
                                                             wait=1, print_log=True);
                    time.sleep(1);
                    # wait a random amount of time (between 60 to 180 seconds), Avg wait would be 120 seconds!
                    time.sleep(random.randint(ADD_WAIT_st, ADD_WAIT_en));

                    # save file
                    with open(filename, 'w', encoding='utf-8') as f:
                        f.write(driver.page_source)
                    # add file details to history file
                    # comment for now
                    # append_entry_in_history_file(filename, hist_cusip=cusip[i], history_file=network_drive + 'html_history.csv')

                    # close the third window and set the handle to second window
                    driver.close()
                    driver.switch_to.window(handles[1])  # starts from 0

                    # undo mouse action
                    summary_tab = '//*[@id="ViewTab"]/div[1]'
                    driver.find_element_by_xpath(summary_tab).click()
                    driver.find_element_by_xpath(summary_tab).click()

                    # check if we are operating in allowed time period
                    check_suitable_time();

                print("Counter:", counter, "Saving", num_art, "articles spanning from: ",
                      new_start_date.strftime("%d-%m-%Y"), " to: ", new_end_date.strftime("%d-%m-%Y"));

            else:
                print("File:", filename, "has already been downloaded!")

            if os.path.exists(completed_file) and os.path.exists(hole_file) and len(holes_start_list) == 0 and len(holes_end_list) == 0:
                # break out of outer while since new_end_date won't be equal to end_file for outer while to end in case of hole_files
                # Also delete the hole_file since all of the dates have been downloaded
                release_file(hole_file)
                break;

            # at the end of successful download set the begining date for next download iteration
            new_start_date = new_end_date + datetime.timedelta(1);
            # time.sleep(0.5);

        # click modify search (after all files have been downloaded for a ticker)
        modify_search(driver);

        # clear free text search box
        send_ticker_to_free_text_box(driver, '');

        # Delete the lock_file that was acquired. Note that I am not marking the ticker complete; that will be done by checks in the beginning of ticker for loop
        release_file(lock_file, log_str='All files downloaded for cusip: ' + cusip[i])

        # all files have been downloaded. Go to next cusip
        print('All files downloaded for tic:', tic, 'Moving to next')


# the idea is to restart browser open and download process if at all an exception occurs in the downloading data
# in below we catch any exception, clear the driver and then restart the process all over again
driver = None
all_verified = all_completed = all_failed = all_folders = all_holes = None  # initialization
while True:
    try:
        wait_till_suitable_time()
        restart_article_download()  # this function will download all files. this will take a lot of time and will hit some exception or the other a lot of times
        break;  # break from the infinite loop when all files for all tickers have been downloaded
    except Exception as ex:
        template = "An exception of type {0} occurred"
        message = template.format(type(ex).__name__)
        print(message)
        # wait for 5 seconds
        time.sleep(5);
        # gracefully clear driver. Need to think about the case when driver doesn't exist
        if driver is not None:
            try:
                driver.window_handles
                for h in driver.window_handles:
                    driver.switch_to.window(h);
                    driver.close()
            except WebDriverException:
                # driver is already closed
                pass
            except InvalidSessionIdException:
                # driver is already closed
                pass
        # make driver None
        driver = None  # global variable
        # wait for 5 more seconds
        time.sleep(5);

# Once the download finishes, the first step is to look for all files with __RECHECK__ tag. These are the dates with more than 100 news articles
# Then check the files which are small in size. These combination of ticker and dates needs to be rerun. This should be done separately after all files have been downloaded
# The above step (3rd one) would be ideally suited when converting html files to text files. In the process identify files with lesser articles
# Next check all files for the stated number of downloads and the actual articles present. Flag the ones with lesser articles and download them again.


