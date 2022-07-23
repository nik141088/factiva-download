# python "C:\Users\nikhi\PycharmProjects\learnPython\greedy_with_subjects.py"
commons_file = "C:\\Users\\nikhi\\Dropbox\\PycharmProjects\\learnPython\\commons.py";

# copy sys.path from pycharm here. It is needed for python (run from cmd) to find installed libraries
site_packages = ['C:\\Program Files\\JetBrains\\PyCharm Community Edition 2020.3.4\\plugins\\python-ce\\helpers\\pydev', 'C:\\Program Files\\JetBrains\\PyCharm Community Edition 2020.3.4\\plugins\\python-ce\\helpers\\third_party\\thriftpy', 'C:\\Program Files\\JetBrains\\PyCharm Community Edition 2020.3.4\\plugins\\python-ce\\helpers\\pydev', 'C:\\Users\\nikhi\\AppData\\Local\\Programs\\Python\\Python39\\python39.zip', 'C:\\Users\\nikhi\\AppData\\Local\\Programs\\Python\\Python39\\DLLs', 'C:\\Users\\nikhi\\AppData\\Local\\Programs\\Python\\Python39\\lib', 'C:\\Users\\nikhi\\AppData\\Local\\Programs\\Python\\Python39', 'C:\\Users\\nikhi\\AppData\\Local\\Programs\\Python\\Python39\\lib\\site-packages', 'C:\\Users\\nikhi\\PycharmProjects\\learnPython', 'C:/Users/nikhi/PycharmProjects/learnPython'];


import sys
for s in site_packages:
    sys.path.append(s)


from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException, InvalidSessionIdException
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.common.exceptions import StaleElementReferenceException
import selenium
import math
import time
import datetime
import glob
import os
import io
import csv
import calendar
import inspect
import pathlib
import getpass
import csv
import difflib
import re
from random import randrange
from random import shuffle
from numpy import random
import pickle


huge_wait = 300
more_wait = 60 # for safer side
less_wait = 10
tiny_wait = 3
num_tries = 20


# the below file contains the below variables
# factiva URL (through iimb library redirection). variables: iimb_url, remotexs_url, iimb_factiva
# login and password (for factiva through iimb). variables: login, password
login_info = "C:\\Users\\nikhi\\Dropbox\\PycharmProjects\\learnPython\\login_info.py";
with open(login_info, "rb") as source_file:
    code = compile(source_file.read(), login_info, "exec")
exec(code)





network_drive = '\\\\LAPTOP-59MM2PIF\\Factiva\\'
# network_drive = '\\\\nikhil-lappy\\Factiva\\'
# network_drive = 'Z:\\'
# network_drive = 'F:\\'
factivia_dir_articles = network_drive + 'html\\';
selenium_dir = network_drive + 'essentials\\'



# the below function gives the seconds since the supplied file was created/accessed
def seconds_since_creation(file):
    curr_time = datetime.datetime.now()
    mtime = os.stat(file).st_mtime
    mtime = datetime.datetime.fromtimestamp(mtime)
    return (curr_time - mtime).seconds



# this function tires to save all existing files in global list and then refer to it when searching inside code.
# this is similar to caching and should help speed up the search process
# the global list file is currently being updated on every new run i.e. when download is restarted
def my_path_exists(file, cusip = None, global_list = None):
    if (cusip is not None) and (global_list is not None) and (cusip in global_list):
        return True
    else:
        return os.path.exists(file)




def send_keys_by_xpath(driver, xpath, keys, wait = 0.25, slow = True, slow_wait = 0.01):
    for p in range(0, len(xpath)):
        # first clear the fields (for safer side)
        driver.find_element_by_xpath(xpath[p]).clear()
        time.sleep(wait)
        if slow == False:
            driver.find_element_by_xpath(xpath[p]).send_keys(keys[p])
        else:
            for char in keys[p]:
                driver.find_element_by_xpath(xpath[p]).send_keys(char)
                time.sleep(slow_wait)



def try_catch_find_element_by_xpath(driver, xpath, fun = "click", info = "", tries = num_tries, wait = 1, print_log = True):
    for p in range(0, len(xpath)):
        xp = xpath[p]
        for nt in range(tries):
            # noinspection PyUnreachableCode
            try:
                if fun == "click":
                    driver.find_element_by_xpath(xp).click()
                if fun == "clear":
                    driver.find_element_by_xpath(xp).clear()
                break
            except Exception as ex:
                template = "An exception of type {0} occurred"
                message = template.format(type(ex).__name__)
                if print_log:
                    print("fun:", inspect.stack()[0][3], "info: ", info, "nt: ", nt, "/", tries, "err:", message)
                if nt == tries - 1:
                    raise ex
                else:
                    if print_log:
                        print(info + "retry in", wait, "sec.")
                    time.sleep(wait)





def try_catch_and_WAIT_find_element_by_xpath(driver, xpath, ec_wait = less_wait, fun = "element_to_be_clickable", info = "", tries = 3, wait = 1, print_log = True):
    for p in range(0, len(xpath)):
        xp = xpath[p]
        for nt in range(tries):
            try:
                if fun == "element_to_be_clickable":
                    elem = WebDriverWait(driver, ec_wait).until(EC.element_to_be_clickable((By.XPATH, xp)))
                    elem.click()
                if fun == "presence_of_element_located":
                    elem = WebDriverWait(driver, ec_wait).until(EC.presence_of_element_located((By.XPATH, xp)))
                    elem.click()
                break
            except StaleElementReferenceException:
                # go again
                pass;
            except TimeoutException:
                if nt == tries - 1:
                    # last try
                    return -1;
                else:
                    # go again
                    pass;
            except Exception as ex:
                template = "An exception of type {0} occurred"
                message = template.format(type(ex).__name__)
                if print_log:
                    print("fun:", inspect.stack()[0][3], "info: ", info, "nt: ", nt, "/", tries, "err:", message)
                if nt == tries - 1:
                    raise ex
                else:
                    if print_log:
                        print(info + "retry in", wait, "sec.")
                    time.sleep(wait)





def check_element_exists_by_xpath(driver, xpath):
    try:
        driver.find_element_by_xpath(xpath)
        if driver.find_element_by_xpath(xpath).text == '':
            return False
    except NoSuchElementException:
        return False
    return True




def check_element_exists_by_class(driver, class_name):
    try:
        driver.find_element_by_class_name(class_name)
        if driver.find_element_by_class_name(class_name).text == '':
            return False
    except NoSuchElementException:
        return False
    return True




# return False on zero matches, 1 on exactly one match and 2 for more than one match
def try_catch_add_company(driver, tic, tries = num_tries, ret = False, print_log = True):

    # first remove existing entry (if any)
    existing_company = '//*[@id="coLst"]/div/ul/li/div/div/span'
    remove_option = '//*[@id="copillscontextmenu"]/div/div[2]/span'
    try:
        driver.find_element_by_xpath(existing_company).is_displayed()
        # remove the element
        try_catch_find_element_by_xpath(driver, [existing_company, remove_option], fun="click", info="company_remove: ")
    except NoSuchElementException:
        # good to go. Do nothing
        pass
    except Exception as ex:
        template = "An exception of type {0} occurred"
        message = template.format(type(ex).__name__)
        if print_log:
            print("fun:", inspect.stack()[0][3], "info: remove ticker", "tic: ", tic, "err:", message)
        raise ex

    # Add stock ticker: tic
    if tic != "":
        company_triangle = '//*[@id="coTab"]/div[1]'
        try_catch_and_WAIT_find_element_by_xpath(driver, [company_triangle], ec_wait=less_wait,
                                                 fun="element_to_be_clickable", info="company_search: ")
        search_bar = '//*[@id="coTxt"]'
        send_keys_by_xpath(driver, [search_bar], [tic])
        # now wait for it to show results
        time.sleep(3);
        first_entry_class = 'ac_descriptor'
        # first_entry_xpath = '/html/body/div[13]/table/tbody/tr[1]/td/div'
        second_entry_xpath = '/html/body/div[13]/table/tbody/tr[2]/td/div'
        num_entries = None

        # Update 20th March 2021: Factiva now seems to have first_entry_class element present even for no matches. It also matches all companies present!
        all_elems = driver.find_elements_by_class_name(first_entry_class);
        cnt = 0;
        for t in range(0, len(all_elems)):
            if all_elems[t].text != '':
                cnt = cnt + 1;
        num_entries = cnt;
        if num_entries == 1:
            # click on the first entry
            all_elems[0].click();
        else:
            # clear company search bar
            driver.find_element_by_xpath(search_bar).clear()
        # turn off company triangle
        driver.find_element_by_xpath(company_triangle).click()

        if ret == True:
            return num_entries
        else:
            return None

        # commenting out the below. Refer this in case of future failures
        """
        no_elem_exception = False
        for nt in range(tries):
            try:
                # driver.find_element_by_xpath(first_entry_xpath)
                driver.find_element_by_class_name(first_entry_class)
                break;
            except NoSuchElementException:
                # there aren't any matches for this ticker/company. Try again!
                if nt == tries-1:
                    no_elem_exception = True
                if print_log:
                    print("fun:", inspect.stack()[0][3], "info: add ticker", "tic: ", tic, "nt: ", nt, "/", tries, "Exception: NoSuchElementException")
                    print("company_search: " + "retry in", 1, "sec.")
                    time.sleep(1)
            except Exception as ex:
                template = "An exception of type {0} occurred"
                message = template.format(type(ex).__name__)
                if print_log:
                    print("fun:", inspect.stack()[0][3], "info: add ticker", "tic: ", tic, "nt: ", nt, "/", tries, "err:", message)
                raise ex


        if no_elem_exception == True:
            # This means that either there is no match OR that factiva couldn't search the name properly. For the second case, try changing the ticker text
            send_keys_by_xpath(driver, [search_bar], ['aapl'])
            time.sleep(3)
            send_keys_by_xpath(driver, [search_bar], [tic])
            time.sleep(3)
            try:
                driver.find_element_by_class_name(first_entry_class).click()
            except NoSuchElementException:
                pass


        if check_element_exists_by_class(driver, first_entry_class) == True:
            # found first entry. Are there more?
            if check_element_exists_by_xpath(driver, second_entry_xpath) == True:
                # there exists a second entry as well
                num_entries = 2
            else:
                num_entries = 1
                # click on the first entry
                driver.find_element_by_class_name(first_entry_class).click();
        else:
            num_entries = 0

        # click on company triangle
        try_catch_and_WAIT_find_element_by_xpath(driver, [company_triangle], ec_wait=less_wait, fun="element_to_be_clickable", info="company_search: ")

        if ret == True:
            return num_entries
        """






def open_factivia_url(chrome_options = webdriver.ChromeOptions(), bypass_maintenance_page = False, bypass_customize_homepage_page = False, factiva_showing_snapshot_page = False):

    driver = webdriver.Chrome(options = chrome_options,
                              executable_path=selenium_dir + "chromedriver.exe")  # this will start the chrome browser

    driver.get(iimb_url)  # get() function waits for the page to load.

    # driver.set_window_rect(-7, 0, 782, 831); # left half of laptop screen
    # driver.set_window_rect(-2567, -216, 1294, 1047); # left half of extendedmonitor

    # path to factivia in iimb database. Current directory: os.getcwd()
    factivia_url = '//*[@id="s-lg-content-47202199"]/div/div/button/span/a'
    try_catch_and_WAIT_find_element_by_xpath(driver, [factivia_url], ec_wait=more_wait, fun="element_to_be_clickable",
                                             info="factivia_url: ")

    if factiva_showing_snapshot_page == True:
        # switch to 2nd page
        handles = driver.window_handles
        driver.switch_to.window(handles[1])
        # click on search
        search_tab = '//*[@id="navmbm0"]/a'
        try_catch_and_WAIT_find_element_by_xpath(driver, [search_tab], ec_wait=more_wait, fun="element_to_be_clickable")


    # Change handle to second page. You may close the first page. Although it is not needed
    handles = driver.window_handles
    driver.switch_to.window(handles[1])  # numbering starts rom 0

    if bypass_customize_homepage_page:
        serach_builder_xpath = '//*[@id="hprGoToCLP"]'
        try_catch_find_element_by_xpath(driver, [serach_builder_xpath], "click", info = "search builder page: ")

    if bypass_maintenance_page:
        ok_button_xpath = '//*[@id="okBtn"]'
        # driver.find_element_by_xpath(ok_button_xpath).click();
        try_catch_find_element_by_xpath(driver, [ok_button_xpath], "click", info = "maintenance page: ")

    return(driver)




def factivia_set_up(driver, __search_form = True, __date_range_type = "Enter date range...",
                    __duplicate_type = "Off", __region_usa = True, __source_set = True, __subject_set = True):

    # for debugging
    # __search_form = True; __date_range_type = "Enter date range..."; __duplicate_type = "Off"; __region_usa = True; __source_set = True; __subject_set = True

    # make sure the page is loaded. wait for search form availability
    search_form = '//*[@id="sfs"]/a'
    if __search_form == True:
        try_catch_and_WAIT_find_element_by_xpath(driver, [search_form], ec_wait=less_wait, fun="element_to_be_clickable", info="search_form: ")

    # select date range
    if __date_range_type is not None:
        date_range = driver.find_element_by_xpath('//*[@id="dr"]')
        date_range_select = Select(date_range)
        date_range_select.select_by_visible_text(__date_range_type)

    # duplicates
    if __duplicate_type is not None:
        duplicates = driver.find_element_by_xpath('//*[@id="isrd"]')
        duplicates_select = Select(duplicates)
        duplicates_select.select_by_visible_text(__duplicate_type)


    # Set region to USA -- some problem
    region_triangle = '//*[@id="reTab"]/div[1]'
    north_america = '//*[@id="reMnu"]/ul/li[15]/span'
    usa = '//*[@id="reMnu"]/ul/li[15]/div/ul/li[6]/a[1]'
    if __region_usa == True:
        try_catch_and_WAIT_find_element_by_xpath(driver, [region_triangle, north_america, usa, region_triangle], ec_wait=less_wait, fun="element_to_be_clickable", info="region: ")


    # Set source to dow jones newswire - institutional
    source_triangle = '//*[@id="scTab"]/div[1]'
    # select top sources from drop down
    source_list = driver.find_element_by_xpath('//*[@id="scCat"]')
    source_list_select = Select(source_list)
    # choose dow jones institutional news
    dj_newswire_triangle = '//*[@id="scMnu"]/ul/li[1]/span/span'
    dj_newswire_all = '//*[@id="scMnu"]/ul/li[1]/a[1]'
    dj_institutional = '//*[@id="scMnu"]/ul/li[1]/div/ul/li[1]/a[1]'
    # choose press release news wires
    pr_wires = '//*[@id="scMnu"]/ul/li[3]/a[1]'
    # choose WSJ
    wsj_triangle = '//*[@id="scMnu"]/ul/li[5]/span/span'
    wsj_all = '//*[@id="scMnu"]/ul/li[5]/a[1]'
    wsj_print = '//*[@id="scMnu"]/ul/li[5]/div/ul/li[6]/a[1]'
    wsj_online = '//*[@id="scMnu"]/ul/li[5]/div/ul/li[5]/a[1]'
    if __source_set == True:
        try_catch_and_WAIT_find_element_by_xpath(driver, [source_triangle], ec_wait=less_wait, fun="element_to_be_clickable", info="source: ")
        source_list_select.select_by_visible_text('Top Sources')
        # try_catch_and_WAIT_find_element_by_xpath(driver, [dj_newswire_triangle, dj_institutional, source_triangle], ec_wait = less_wait, fun = "element_to_be_clickable", info = "source: ")
        # try_catch_and_WAIT_find_element_by_xpath(driver, [pr_wires, source_triangle], ec_wait = less_wait, fun = "element_to_be_clickable", info = "source: ")
        # try_catch_and_WAIT_find_element_by_xpath(driver, [wsj_triangle, wsj_print, source_triangle], ec_wait = less_wait, fun = "element_to_be_clickable", info = "source: ")
        try_catch_and_WAIT_find_element_by_xpath(driver, [dj_newswire_all, pr_wires, wsj_all, source_triangle], ec_wait=less_wait, fun="element_to_be_clickable", info="source: ")


    # Set subject to equity markets and corporate events
    subject_triangle = '//*[@id="nsTab"]/div[1]'
    fin_mkt_news = '//*[@id="nsMnu"]/ul/li[1]/span/span'
    equity_markets = '//*[@id="nsMnu"]/ul/li[1]/div/ul/li[5]/a[1]'
    corp_events = '//*[@id="nsMnu"]/ul/li[3]/a[1]'
    if __subject_set == True:
        try_catch_and_WAIT_find_element_by_xpath(driver, [subject_triangle, fin_mkt_news, equity_markets, corp_events, subject_triangle],
                                                 ec_wait=less_wait, fun="element_to_be_clickable", info="subject: ")

    # language is automatically set to english, if not use below
    existing_lang = '//*[@id="laLst"]/div/ul/li/div/div/span'
    try:
        driver.find_element_by_xpath(existing_lang).is_displayed()
        # remove the element
        try_catch_find_element_by_xpath(driver, [existing_lang], fun="click", info = "language_remove: ")
    except NoSuchElementException:
        # good to go. Do nothing
        pass
    except Exception as ex:
        template = "An exception of type {0} occurred"
        message = template.format(type(ex).__name__)
        print(message)
    language_triangle = '//*[@id="laTab"]/div[1]'
    english = '//*[@id="laMnu"]/ul/li[12]/a'
    try_catch_and_WAIT_find_element_by_xpath(driver, [language_triangle, english, language_triangle], ec_wait=less_wait,
                                             fun="element_to_be_clickable", info="language: ")


    return(driver)





def factiva_set_subject(driver, subject_xpath):
    # Set subject to equity markets and corporate events
    l = list();
    subject_triangle = '//*[@id="nsTab"]/div[1]'
    l.append(subject_triangle);
    for s in subject_xpath:
        if isinstance(s, list):
            for t in s:
                l.append(t)
        else:
            l.append(s);
    l.append(subject_triangle);
    try_catch_and_WAIT_find_element_by_xpath(driver, l,
                                             ec_wait = more_wait, fun = "element_to_be_clickable", info = "subject: ")




def open_factivia_from_remotexs(chrome_options = webdriver.ChromeOptions(), bypass_maintenance_page = False, bypass_customize_homepage_page = False, factiva_showing_snapshot_page = False):

    driver = webdriver.Chrome(options = chrome_options,
                              executable_path=selenium_dir + "chromedriver.exe")  # this will start the chrome browser

    driver.get(remotexs_url)  # get() function waits for the page to load.

    login_xpath = '//*[@id="edit-name"]';
    pass_xpath = '//*[@id="edit-pass"]';
    login_button_xpath = '//*[@id="edit-submit"]';
    # enter login
    driver.find_element_by_xpath(login_xpath).clear();
    send_keys_by_xpath(driver, [login_xpath], [login]);
    # enter pass
    driver.find_element_by_xpath(pass_xpath).clear();
    send_keys_by_xpath(driver, [pass_xpath], [password]);
    # press log-in button
    driver.find_element_by_xpath(login_button_xpath).click();

    # driver.set_window_rect(-7, 0, 782, 831); # left half of laptop screen
    # driver.set_window_rect(-2567, -216, 1294, 1047); # left half of extendedmonitor

    # jump to factiva section
    driver.get(iimb_factiva);

    # path to factivia in iimb database. Current directory: os.getcwd()
    factivia_url = '//*[@id="s-lg-content-47202199"]/div/div/button/span/a'
    try_catch_and_WAIT_find_element_by_xpath(driver, [factivia_url], ec_wait=more_wait, fun="element_to_be_clickable",
                                             info="factivia_url: ")

    if factiva_showing_snapshot_page == True:
        # switch to 2nd page
        handles = driver.window_handles
        driver.switch_to.window(handles[1])
        # click on search
        search_tab = '//*[@id="navmbm0"]/a'
        try_catch_and_WAIT_find_element_by_xpath(driver, [search_tab], ec_wait=more_wait, fun="element_to_be_clickable")


    # Change handle to second page. You may close the first page. Although it is not needed
    handles = driver.window_handles
    driver.switch_to.window(handles[1])  # numbering starts rom 0

    if bypass_customize_homepage_page:
        serach_builder_xpath = '//*[@id="hprGoToCLP"]'
        try_catch_find_element_by_xpath(driver, [serach_builder_xpath], "click", info = "search builder page: ")

    if bypass_maintenance_page:
        ok_button_xpath = '//*[@id="okBtn"]'
        # driver.find_element_by_xpath(ok_button_xpath).click();
        try_catch_find_element_by_xpath(driver, [ok_button_xpath], "click", info = "maintenance page: ")

    return(driver)









# This function clicks modify-search (to go back to the search page again).
# Then it enters the new dates supplied. It loads the page and fetches the number of articles found.
# It also works when there are no articles present for a page. It is slower when number of articles are 0.
# Note that it doesn't go back to the search page back again.
def try_new_date(driver, start_date, end_date, skip_modify_search = False, change_dates = True, num_tries = 3, print_log = True, dir_search = "", tic_search = "", ret_first_date = False, ret_hundreth_date = False):

    # if the file already exists then we can simply return the number of articles stored in the html file
    if (dir_search != "") and (tic_search != "") and (skip_modify_search is False):
        all_files = glob.glob(dir_search + "_".join([tic_search, "from", start_date.strftime("%Y%m%d"), "to", end_date.strftime("%Y%m%d")]) + "*.html");
        if len(all_files) == 1:
            print("File: ", all_files[0], "already exists. Getting num_art from filename!");
            tmp = all_files[0].replace(".html", "").replace("__RECHECK__", "").split("_")[7];
            num_art = int(tmp);
            # here it doesn't make sense to return first and/or hundreth date
            return (num_art, None, None);

    if skip_modify_search:
        pass
    else:
        time.sleep(1); # this wait was added to slow things down!
        modify_search(driver);


    if change_dates == True:
        date_keys = [str(start_date.day), str(start_date.month), str(start_date.year), str(end_date.day), str(end_date.month), str(end_date.year)]
        date_paths = ['//*[@id="frd"]', '//*[@id="frm"]', '//*[@id="fry"]', '//*[@id="tod"]', '//*[@id="tom"]',
                      '//*[@id="toy"]']
        if print_log:
            print(date_keys)
        send_keys_by_xpath(driver, date_paths, date_keys)


    driver.find_element_by_xpath(search_button).click()
    ret = try_catch_and_WAIT_find_element_by_xpath(driver, [num_articles_text], ec_wait=less_wait,
                                                   fun="presence_of_element_located", info="", tries = num_tries, wait=5, print_log = True)
    if ret == -1:
        # maybe there are no articles. In this case search for the "No search results" string
        my_str = driver.find_element_by_xpath('//*[@id="headlines"]').text.strip();
        if my_str[0:17] == "No search results":
            # all okay. there really are no articles. Here also it doesn't make sense to return first and/or hundreth date
            return (0, None, None)

    time.sleep(1)
    txt = driver.find_element_by_xpath(num_articles_text).text
    txt = txt.split(' of ')[1]
    txt = txt.replace(",", "")
    num_art = int(txt)

    # now find first and hundreth date of searched articles
    first_art_date = extract_date(driver, first_art_date_text);
    if num_art > 100:
        hundreth_date = extract_date(driver, hundreth_art_date_text);
    else:
        hundreth_date = None

    return (num_art, first_art_date, hundreth_date);









def try_new_date_recheck(driver, start_date, end_date, click_modify_search = False, change_dates = True, num_tries = 3, print_log = True):

    if change_dates == True:
        date_keys = [str(start_date.day), str(start_date.month), str(start_date.year), str(end_date.day), str(end_date.month), str(end_date.year)]
        date_paths = ['//*[@id="frd"]', '//*[@id="frm"]', '//*[@id="fry"]', '//*[@id="tod"]', '//*[@id="tom"]',
                      '//*[@id="toy"]']
        if print_log:
            print(date_keys)
        send_keys_by_xpath(driver, date_paths, date_keys)

    driver.find_element_by_xpath(search_button).click()
    ret = try_catch_and_WAIT_find_element_by_xpath(driver, [num_articles_text], ec_wait=less_wait,
                                                   fun="presence_of_element_located", info="", tries = num_tries, wait=5, print_log = True)
    if ret == -1:
        # maybe there are no articles. In this case search for the "No search results" string
        my_str = driver.find_element_by_xpath('//*[@id="headlines"]').text.strip();
        if my_str[0:17] == "No search results":
            if click_modify_search:
                time.sleep(1);  # this wait was added to slow things down!
                modify_search(driver);
            # all okay. there really are no articles. Here also it doesn't make sense to return first and/or hundreth date
            return (0)

    time.sleep(1)
    txt = driver.find_element_by_xpath(num_articles_text).text
    txt = txt.split(' of ')[1]
    txt = txt.replace(",", "")
    num_art = int(txt)

    if click_modify_search:
        time.sleep(1);  # this wait was added to slow things down!
        modify_search(driver);

    return (num_art)










# extract date from date text
def extract_date(driver, date_text):
    date = None
    txt = driver.find_element_by_xpath(date_text).text
    txt = txt.split(', ')
    # try-catch to find the date
    for i in range(0, len(txt)):
        try:
            date = datetime.datetime.strptime(txt[i], '%d %B %Y')
        except ValueError:
            pass
    return date



# Check whether the supplied directory has no holes in it
def check_holes(dir_name, start_date = datetime.datetime(1990, 1, 1), end_date = datetime.datetime(2020, 12, 31), ret = 'status'):

    # directory must exists
    if not os.path.exists(dir_name):
        raise FileNotFoundError

    if ret == 'list':
        l1 = list();
        l2 = list();

    all_files = glob.glob(dir_name + "*.html");
    n = len(all_files);
    # sort all files
    all_files.sort();
    if n == 0:
        # no files present. But by definition there is a hole of the entire date range
        if ret == 'list':
            l1.append(start_date)
            l2.append(end_date)
        else:
            # return only status
            return True
    else:
        # Now to validate that there are no holes the files must satisfy three criterion
        # 1) first_date of first file (0-th file) must match start_date
        # 2) last_date of last file ((n-1)-st file) must match end_date
        # 3) first_date of (i+1)-th file must be one plus the last_date of i-th file. This must hold for i in [0, 1, ... , n-2]
        # if any of the above condition fails then there is a hole
        # cond-1
        txt_1 = all_files[0].replace('.html', '').replace('__RECHECK__', '').split('\\')[-1].split('_')[2]
        txt_st = datetime.datetime(int(txt_1[0:4]), int(txt_1[4:6]), int(txt_1[6:8]))
        if txt_st != start_date:
            # there is a hole from start_date to txt_st - 1
            if ret == 'list':
                l1.append(start_date)
                l2.append(txt_st - datetime.timedelta(1))
            else:
                # return only status
                return True
        # cond-2
        txt_2 = all_files[n-1].replace('.html', '').replace('__RECHECK__', '').split('\\')[-1].split('_')[4]
        txt_en = datetime.datetime(int(txt_2[0:4]), int(txt_2[4:6]), int(txt_2[6:8]))
        if txt_en != end_date:
            # there is a hole from txt_en + 1 to end_date
            if ret == 'list':
                l1.append(txt_en + datetime.timedelta(1))
                l2.append(end_date)
            else:
                # return only status
                return True
        # cond-3: only true if there are more than 1 file
        if n > 1:
            repeat_loop = True;
            new_i = 0;
            new_n = n;
            while repeat_loop:
                for i in range(new_i, new_n - 1):
                    txt_1 = all_files[i + 1].replace('.html', '').replace('__RECHECK__', '').split('\\')[-1].split('_')[2]
                    txt_2 = all_files[i].replace('.html', '').replace('__RECHECK__', '').split('\\')[-1].split('_')[4]
                    txt_st = datetime.datetime(int(txt_1[0:4]), int(txt_1[4:6]), int(txt_1[6:8]))
                    txt_en = datetime.datetime(int(txt_2[0:4]), int(txt_2[4:6]), int(txt_2[6:8]))
                    # Due to some errors it may happen that txt_st <= txt_en, i.e. start_date of (i+1)-th file is less than or equal to end_date of i-th file
                    # In such cases, we must remove the corrupted file and continue
                    if txt_st <= txt_en:
                        # remove (i+1)-th file
                        release_file(all_files[i + 1]);
                        # remove (i+2)-th entry from all_files. Note that all_files[i+1] represents (i+2)-th entry in all_files
                        all_files = all_files[:(i + 1)] + all_files[(i + 2):];
                        # reduce i, set n and break from for
                        new_n = new_n - 1;
                        new_i = i;
                        break;
                    if txt_st != txt_en + datetime.timedelta(1):
                        # there is a hole between txt_en + 1 to txt_st - 1
                        if ret == 'list':
                            l1.append(txt_en + datetime.timedelta(1))
                            l2.append(txt_st - datetime.timedelta(1))
                        else:
                            # return only status
                            return True
                    if i == new_n - 2:
                        repeat_loop = False;

    # no errors found. this directory doesn't have any holes
    if ret == 'list':
        return [l1, l2]
    else:
        return False



# Release/delete a file
def release_file(file, forced = True, log = True, log_str = ''):
    if os.path.exists(file):
        if forced == True:
            # make sure you can close a file opened by another process
            f = open(file, "w");
            f.close();
        os.remove(file)
        if log == True:
            print_str = 'Releasing file: ' + file + '. ' + log_str
            print(print_str)





# some important xpaths.
search_button = '//*[@id="btnSearchBottom"]';
modify_search_button = '//*[@id="btnModifySearch"]';
date_xpath = '//*[@id="dateAndDupRow"]/div[1]/table/tbody/tr/td[1]/label';
num_articles_text = '//*[@id="headlineHeader33"]/table/tbody/tr/td/span[2]';
first_art_date_text = '//*[@id="headlines"]/table/tbody/tr[1]/td[3]/div[1]';
hundreth_art_date_text = '//*[@id="headlines"]/table/tbody/tr[100]/td[3]/div[1]';

# start and end dates to be used in Factiva downloads
start_date = datetime.datetime(1990, 1, 1);
end_date = datetime.datetime(2020, 12, 31);
TOTAL_DAYS = (end_date - start_date).days + 1;



def send_ticker_to_free_text_box(driver, ticker, only_ret_curr_text = False):
    # click the free-text tab
    driver.find_element_by_xpath('//*[@id="fts"]/a').click();
    # clear the current text
    curr_text = driver.find_element_by_xpath('//*[@id="editor"]').text;
    if only_ret_curr_text:
        return curr_text
    for c in range(0, len(curr_text)):
        driver.find_element_by_xpath('//*[@id="editor"]/textarea').send_keys(Keys.BACKSPACE);
    # now enter the new ticker
    driver.find_element_by_xpath('//*[@id="editor"]/textarea').send_keys(ticker)
    if ticker != '':
        # enter random characters to avoid help box from showing up
        random_chars = 'jihgfedcba'
        driver.find_element_by_xpath('//*[@id="editor"]/textarea').send_keys(random_chars)
        # now delete the random chars
        for c in range(0, len(random_chars)):
            driver.find_element_by_xpath('//*[@id="editor"]/textarea').send_keys(Keys.BACKSPACE);


# remove some common issues from the company name
def sanitize(name):
    # any text in last set of paranthesis
    regex = r'\s\([^)]+\)$'
    name = re.sub(regex, '', name)
    # slash followed by characters at the end
    regex = r'\s*/([^/]+)'
    name = re.sub(regex, '', name)
    # change inc, incorporated to inc
    regex = r'\sINC\b|\sINCORPORATED\b'
    name = re.sub(regex, ' INC', name)
    # change corp, corporation, corporated to corp
    name = re.sub(regex, ' CORP', name)
    # change international to intl
    regex = r'\sINTERNATIONAL\b'
    name = re.sub(regex, ' INTL', name)
    # change company, companies to co
    regex = r'\sCOMPANY\b|\sCOMPANIES\b'
    name = re.sub(regex, ' CO', name)
    return name



# it seems similar to lcs similarity. See a R function: stringdist::stringsim(first, second, "lcs")
def similarity(first, second):
    first = first.upper()
    second = second.upper()
    first = sanitize(first)
    second = sanitize(second)
    # special (non-word) characters
    regex = r'[^\w]'
    first = re.sub(regex, '', first)
    second = re.sub(regex, '', second)
    return difflib.SequenceMatcher(None, first, second).ratio()



def modify_search(driver):
    driver.find_element_by_xpath(modify_search_button).click()
    try_catch_and_WAIT_find_element_by_xpath(driver, [date_xpath], ec_wait=less_wait,
                                             fun="presence_of_element_located", info="", tries=5, wait=1, print_log=True);



# add file details to history file
def append_entry_in_history_file(filename, hist_cusip, history_file = network_drive + 'html_history.csv'):
    hist_stat = pathlib.Path(filename).stat();
    hist_mtime = int(hist_stat.st_mtime);  # need to do as.POSIXct(hist_mtime, origin = origin) in R. In Python do: datetime.datetime.fromtimestamp(hist_mtime)
    hist_size_kb = round(hist_stat.st_size / 1e3);
    hist_num_art = int(filename.split('_')[7][0:5]);
    hist_st_date = '-'.join(
        [filename.split('_')[2][0:4], filename.split('_')[2][4:6], filename.split('_')[2][6:8]]);
    hist_en_date = '-'.join(
        [filename.split('_')[4][0:4], filename.split('_')[4][4:6], filename.split('_')[4][6:8]]);
    hist_RECHEK = int(bool(re.search('__RECHECK__', filename)));
    # Load html hostory file for appending
    hist_TEXT = [filename, hist_mtime, hist_size_kb, hist_num_art, hist_st_date, hist_en_date,
                 hist_cusip, hist_RECHEK]
    with open(history_file, "a", encoding="utf8") as hist_file:
        writer = csv.writer(hist_file)
        writer.writerow(hist_TEXT)



def word_count_bins(N, K = 0):
    # model: WC = 23470 / (N**0.44). Valid for WC between 100 and 25000
    if K == 0:
        K = math.ceil(N/90); # roughly 90 articles per bin
    K = K-1;
    start = list();
    end = list();
    for j in range(K):
        st = 23740 / (((j+1)/K)*N)**0.44;
        if j==0:
            en = 1e9;
        else:
            en = 23740 / (((j)/K)*N)**0.44;

        start.append(int(st))
        end.append(int(en-1))

    start.append(int(0));
    end.append(int(st-1));

    start.reverse();
    end.reverse();

    return [start, end]




# R style unique and uniqueN
def unique(l):
    if isinstance(l, list) is False:
        print('object is not list')
        assert 0
    if len(l) > 1:
        tmp = set(l)
        tmp2 = list(tmp)
        tmp2.sort()
        return tmp2
    else:
        return l

def uniqueN(l):
    if isinstance(l, list) is False:
        print('object is not list')
        assert 0
    if len(l) > 1:
        tmp = set(l)
        N = len(tmp)
        return N
    else:
        return len(l)


