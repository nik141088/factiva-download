# python "C:\Users\nikhi\PycharmProjects\learnPython\greedy_with_subjects.py"
import pathlib
import pickle
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




# Load the recheck files
f = open(selenium_dir + 'recheck_missing.csv', "r", encoding="utf8")
temp = f.readlines()
f.close()
recheck_cusip = [temp[i].strip("\n").split(",")[0] for i in range(1, len(temp))]
recheck_st_date = [temp[i].strip("\n").split(",")[1] for i in range(1, len(temp))]
recheck_en_date = [temp[i].strip("\n").split(",")[2] for i in range(1, len(temp))]
recheck_num_art = [temp[i].strip("\n").split(",")[3] for i in range(1, len(temp))]
# below is to connect back to factiva_sample_firms_with_tags dataset
main_idx = [cusip.index(recheck_cusip[i]) for i in range(0, len(recheck_cusip))]


iter_j = [i for i in range(0, len(recheck_cusip))]


RECHECK_DIR = network_drive + 'recheck\\';
RECHECK_DONE_DIR = network_drive + 'recheck\\done\\';
RECHECK_WC_DIR = network_drive + 'recheck\\wc_filenames\\';


def restart_recheck_download():

    global driver  # driver is a global variable where multiple runs of restart_article_download() utilize the same variable

    driver = open_factivia_from_remotexs(bypass_maintenance_page=False, bypass_customize_homepage_page=False, factiva_showing_snapshot_page=True);
    factivia_set_up(driver, __date_range_type=None, __source_set=False,
                    __subject_set=False);  # Do not set any source or subject. I will set Date later

    # Loop through all tickers
    for j in iter_j:

        # set i wrt to main_idx
        i = main_idx[j]

        check_suitable_time();

        print("j =", j, "out of", len(iter_j),
              "i =", i, "out of", len(all_tickers),
              "tic =", all_tickers[i],
              "cusip =", cusip[i],
              "compustat_name =", all_conm[i],
              "factiva_tag =", factiva_tag[i],
              "factiva_sim_cos =", '' if factiva_sim_cos[i] in ('', '""') else f"{float(factiva_sim_cos[i]):0.2f}");

        tic = all_tickers[i];
        conm = all_conm[i];
        done_file = RECHECK_DONE_DIR + '__' + cusip[i] + '__' + recheck_st_date[j] + '__.DONE';
        wc_file = RECHECK_WC_DIR + '__' + cusip[i] + '__' + recheck_st_date[j] + '__.WC';

        if os.path.exists(done_file):
            print('done_file exists, skipping to next j')
            continue

        # set the lower and upper limit for WC
        WC_lo = 0
        WC_hi = 99999999

        # fds extra_text: the source "Knobias" seems a faulty source and needs to be excluded from searches. This is particularly problemetic with RECHECK files
        fds_extra_text = '(wc>=' + str(WC_lo) + ' and wc<' + str(WC_hi) + ' not SN=Knobias)';

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
                # continue to next ticker
                continue


        # select manual date entering
        date_range = driver.find_element_by_xpath('//*[@id="dr"]')
        date_range_select = Select(date_range)
        date_range_select.select_by_visible_text("Enter date range...")

        # start and end date
        st_date = datetime.datetime(int(recheck_st_date[j][0:4]), int(recheck_st_date[j][5:7]), int(recheck_st_date[j][8:10]));
        en_date = datetime.datetime(int(recheck_en_date[j][0:4]), int(recheck_en_date[j][5:7]), int(recheck_en_date[j][8:10]));
        if st_date != en_date:
            assert False

        # search number of articles
        time.sleep(5)
        num_art = try_new_date_recheck(driver, st_date, en_date, click_modify_search = True, change_dates = True);
        print('Found number of articles to be', num_art, 'on date', st_date);

        # list of WC combinations and filenames for further processing. Try to read from disk if avialable
        wc_filenames = list()
        if os.path.exists(wc_file):
            print("wc_filenames exists! Reading it from disk from:", wc_file)
            with open(wc_file, 'rb') as fp:
                wc_filenames = pickle.load(fp)
        else:
            # populate wc_filenames by searching
            wc_start = list([0])
            wc_end = list([256])
            wc_art = list()
            rem_art = num_art
            curr_fds_text = send_ticker_to_free_text_box(driver, ticker='', only_ret_curr_text=True)
            # start with a 256 difference
            l = 0;
            while True:
                new_fds_text = re.sub('wc>=\\d* ', 'wc>=' + str(wc_start[l]) + ' ', curr_fds_text)
                new_fds_text = re.sub('wc<\\d* ', 'wc<' + str(wc_end[l]) + ' ', new_fds_text)
                curr_range = wc_end[l] - wc_start[l]
                # update fds field
                send_ticker_to_free_text_box(driver, new_fds_text);
                this_art = try_new_date_recheck(driver, st_date, en_date, click_modify_search=True, change_dates=False);

                if this_art <= 25:
                    # double the next range and mark okay!
                    wc_start.append(wc_end[l])
                    wc_end.append(wc_end[l] + 2 * curr_range)
                    rem_art = rem_art - this_art
                    wc_art.append(this_art)
                    l = l + 1
                elif this_art <= 100:
                    # keep the same range and mark okay
                    wc_start.append(wc_end[l])
                    wc_end.append(wc_end[l] + 1 * curr_range)
                    rem_art = rem_art - this_art
                    wc_art.append(this_art)
                    l = l + 1
                else:
                    # if more than hundred then half the curr_range and set not okay
                    # no need to update wc_start and l
                    wc_end[l] = wc_start[l] + int(curr_range / 2)

                if rem_art <= 100:
                    # we are done
                    wc_end[l] = WC_hi
                    wc_art.append(rem_art)
                    break
                else:
                    continue

            # file names
            for l in range(0, len(wc_art)):
                if wc_art[l] == 0:
                    continue
                elif wc_art[l] > 100:
                    assert 0
                else:
                    fname = RECHECK_DIR + cusip[i] + '\\' + "_".join(
                        [tic, "from", st_date.strftime("%Y%m%d"), "to", en_date.strftime("%Y%m%d"), "num_art",
                         f"{wc_art[l]:05d}", "~".join(["WC", f"{wc_start[l]:08d}", f"{wc_end[l]:08d}"])]) + ".html"

                wc_filenames.append(fname)

            # store wc_filenames for this combination of cusip and st_date
            print("Writing wc_filenames contents to disk to file:", wc_file)
            with open(wc_file, 'wb') as fp:
                pickle.dump(wc_filenames, fp)




        # download all files from wc_filenames
        for l in range(0, len(wc_filenames)):

            fname = wc_filenames[l]

            # fetch start and end WC from filename
            tmp = fname.split('\\')[6]
            tmp = tmp.split('_')[8]
            tmp = tmp.rstrip('.html')
            WC_start = int(tmp.split('~')[1])
            WC_end   = int(tmp.split('~')[2])

            # update fds text
            new_fds_text = re.sub('wc>=\\d* ', 'wc>=' + str(WC_start) + ' ', curr_fds_text)
            new_fds_text = re.sub('wc<\\d* ', 'wc<' + str(WC_end) + ' ', new_fds_text)
            send_ticker_to_free_text_box(driver, new_fds_text);

            this_art = try_new_date_recheck(driver, st_date, en_date, click_modify_search = False, change_dates = True);

            if this_art > 100:
                assert 0

            if this_art == 0:
                modify_search(driver);
                continue

            for d in range(0, this_art):

                doc_id = driver.find_element_by_xpath('//*[@id="headlines"]/table/tbody/tr[' + str(d+1) + ']/td[3]/div[3]').text
                doc_id = re.sub('^\\(Document ', '', doc_id)
                doc_id = re.sub('\\)$', '', doc_id)
                this_checkbox_xpath = '//*[@id="' + doc_id + '"]'

                print('cusip = ' + cusip[i] +
                      ',  tic = ' + tic +
                      ',  j = ' + str(j) + ' / ' + str(len(iter_j)) +
                      ',  l = ' + str(l) + ' / ' + str(len(wc_filenames)) +
                      ',  d = ' + str(d) + ' / ' + str(this_art) +
                      ' doc_id = ' + doc_id);

                # download file
                filename = fname.rstrip('.html')
                # filename = filename + '_' + f"{(d+1):05d}" + '_of_' + f"{this_art:05d}" + '.html';
                filename = filename + '_' + doc_id + '.html';

                if os.path.exists(filename):
                    continue

                # set display options to include indexing
                driver.find_element_by_xpath('//*[@id="ViewTab"]/a').click()
                driver.find_element_by_xpath('//*[@id="dfdCtrl"]/li[3]').click()

                # check relevent checkbox
                if driver.find_element_by_xpath(this_checkbox_xpath).is_selected() is False:
                    try_catch_find_element_by_xpath(driver, [this_checkbox_xpath], "click", info="this checkbox: ")

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

                # uncheck relevent checkbox
                if driver.find_element_by_xpath(this_checkbox_xpath).is_selected() is True:
                    try_catch_find_element_by_xpath(driver, [this_checkbox_xpath], "click", info="this checkbox: ")

            # end d-for



            # go to search page
            modify_search(driver);

            # clear free text search box
            send_ticker_to_free_text_box(driver, '');

            # wait a random amount of time (between 60 to 180 seconds), Avg wait would be 120 seconds!
            time.sleep(random.randint(ADD_WAIT_st, ADD_WAIT_en));

        # end l-for

        # create done file
        file = open(file = done_file, mode = 'w')
        file.write('')
        file.close()



    # end j-for





# the idea is to restart browser open and download process if at all an exception occurs in the downloading data
# in below we catch any exception, clear the driver and then restart the process all over again
driver = None
while True:
    try:
        wait_till_suitable_time()
        # Randomized iteration
        shuffle(iter_j)
        restart_recheck_download()  # this function will download all files. this will take a lot of time and will hit some exception or the other a lot of times
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





