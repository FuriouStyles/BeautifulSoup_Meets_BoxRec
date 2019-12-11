from bs4 import BeautifulSoup as bs
from itertools import cycle
import traceback
import time
from time import sleep
import pandas as pd
import numpy as np
import csv
import requests
import lxml
import re
import random

#instantiate blank pandas dataframes 'boxers' and 'bouts' to store our scraping results in
boxers = pd.DataFrame(columns=['status', 'career', 'titles held', 'birth name', 'alias', 'born', 'nationality', 'debut', 'division', 'stance', 'height', 'reach', 'residence', 'birth place', 'manager/agent', 'name', 'wins', 'losses', 'draws', 'promoter'])
all_bouts = pd.DataFrame(columns=['date', 'opponent', 'w-l-d', 'venue', 'result', 'decision', 'opponent_0'])
head = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.70 Safari/537.36'}

ips = []
proxy_pool = cycle(ips)
search_results_page_attempt = []
search_results_page_error = []
profile_errors = []
bout_errors = []
get_result_soup_errors = []

def get_boxer_profile(boxer, url):
    #use try/except to build the boxer's profile from res_soup.
    try:
        name = boxer.table.h1.text
        record_w = boxer.table.select('.bgW')[0].text
        record_l = boxer.table.select('.bgL')[0].text
        record_d = boxer.table.select('.bgD')[0].text
        br_id = boxer.table.h2.text
        br_id = br_id.strip('ID# ')
        tables = boxer.find_all('table')
        profile = str(tables)
        profile_tables =  pd.read_html(profile)
        table_one = profile_tables[2]
        table_two = profile_tables[3]
        table_one = table_one.drop([0, 1, 2, 5], axis=0).reset_index(drop=True).set_index(0)
        table_two = table_two.drop([0, 1, 2], axis=0).reset_index(drop=True).set_index(0)
        table_two = table_two[:-2]
        profile = table_one.append(table_two)
        profile = profile.T
        profile['name'] = name
        profile['wins'] = record_w
        profile['losses'] = record_l
        profile['draws'] = record_d
        profile['br_id'] = br_id
        profile = profile[profile.columns.drop(list(profile.filter(regex='register')))]
        if 'KOs' in profile.columns:
            profile.drop(columns='KOs', inplace=True)
        else:
            pass
        if 'MMA' in profile.columns:
            profile.drop(columns='MMA', inplace=True)
        else:
            pass
        if 'sex' in profile.columns:
            pass
        else:
            profile['sex'] = 'male'
        print(f"Appending {name} to the Boxers DataFrame")
        global boxers
        boxers = boxers.append(profile, ignore_index=True) # This will append the boxer's profile to the main boxers dataframe that contains all of the boxer profiles we scrape
    except Exception as ex:
        #any errors during the profile build process adds the boxer's boxrec id to the profile_errors set.
        print(f"There was a problem with {name}'s profile: {ex}")
        profile_errors.append([br_id, ex, 'profile'])



def get_bouts(boxer, url):
    #attempts to build the bout list for each boxer from the res_soup
    try:
        name = boxer.table.h1.text
        br_id = boxer.table.h2.text
        br_id = br_id.strip('ID# ')
        people = boxer.select(".personLink")
        people = [i['href'] for i in people]
        opponent_br_id = [i[-6:] for i in people if 'proboxer' in i]
        tables = boxer.find_all('table')
        profile = str(tables)
        bouts = pd.read_html(profile)
        bouts = bouts[8]
        title_rows = bouts['w-l-d'].str.contains('Title').shift(-1)
        search_for = ['Title', 'google', 'scheduled']
        bouts = bouts[~bouts['w-l-d'].str.contains('|'.join(search_for), na=False)]
        bouts.drop(columns=['last 6', 'Unnamed: 0', 'Unnamed: 2', 'Unnamed: 9', 'Unnamed: 10'], inplace=True)
        bouts.rename(columns={'result': 'decision'}, inplace=True)
        bouts.rename(columns={'Unnamed: 8': 'result', 'Unnamed: 6': 'venue'}, inplace=True)
        bouts.dropna(thresh=4, inplace=True)
        bouts['opponent_0'] = name
        bouts['opponent_0_br_id'] = br_id
        bouts['title_fight'] = title_rows
        bouts['opponent_br_id'] = opponent_br_id
        bouts.fillna(value={'title_fight': False})
        print(f"Appending bout list for {name} to the All Bouts DataFrame")
        global all_bouts
        all_bouts = all_bouts.append(bouts, ignore_index=True) # Appends the boxers bouts to the all_bouts dataframe
    except Exception as ex:
        #any errors during the bout list build process adds the boxer's boxrec id to the bout_errors set
        print(f"There was an exception with {name}'s bout sheet: {ex}")
        bout_errors.append([br_id, ex, 'bouts'])



def get_boxer_soup(proxy, suffix):
    print("Getting Boxer Soup")
    boxer_url = 'https://boxrec.com' + suffix
    boxer_content = requests.get(boxer_url, headers=head, timeout=5, proxies=proxy).content
    boxer = bs(boxer_content, features='lxml')
    get_boxer_profile(boxer, boxer_url)
    get_bouts(boxer, boxer_url)
    sleeper = random.randint(3, 5)
    time.sleep(sleeper)


def get_result_soup(soup, proxies):
    #build soup out of the search results in scrape_boxrec that match the class '.personLink'
    results_link_soup = soup.select('.personLink')
    results_list = [i['href'] for i in results_link_soup]
    for i in results_list:
        try:
            get_boxer_soup(proxies, i)
        except Exception as ex:
            get_result_soup_errors.append(i)
            print(f'There was an error in get_result_soup: {ex}')


def search_results_loop(page_num):
    ips = []
    get_proxy_list()
    proxy = next(proxy_pool)
    proxies = {
      'http': proxy,
      'https': proxy
    }
    if check(proxy, 3) == True:
        #check the proxy to see if it's working
        url = 'https://boxrec.com/en/locations/people?l%5Brole%5D=proboxer&l%5Bdivision%5D=&l%5Bcountry%5D=&l%5Bregion%5D=&l%5Btown%5D=&l_go=&offset=' + str(page_num)
        try:
            results_source = requests.get(url, headers=head, timeout=5, proxies=proxies)
            print(f"Status: {results_source.status_code}. Good to go.")
            results_content = results_source.text
            res_soup = bs(results_content, 'lxml')
            get_result_soup(res_soup, proxies)
            global boxers, all_bouts
            print("Appending BOXERS and ALL_BOUTS to csv file. Then resetting both dataframes to empty")
            boxers.to_csv(r'C:\Users\Vicente\Documents\Projects\BoxRec_BS4\BeautifulSoup_Meets_BoxRec\boxers.csv', index=False, mode='a', na_rep='NaN')
            all_bouts.to_csv(r'C:\Users\Vicente\Documents\Projects\BoxRec_BS4\BeautifulSoup_Meets_BoxRec\all_bouts.csv', index=False, mode='a', na_rep='NaN')
            boxers = boxers.iloc[0:0]
            all_bouts = all_bouts.iloc[0:0]
            time_to_sleep = random.randint(5, 15)
            print(f"Sleeping for {time_to_sleep} seconds.")
            time.sleep(time_to_sleep)
        except Exception as ex:
            print(ex)
    else:
        #if the proxy is bad during the check(proxy) stage, append search_results_pages_error set
        print("Bad Proxy when processing search results. Moving On")
        search_results_page_error.append(page_num)


def scrape_boxrec():
    #shuffle the full set of search result search_result_pages so requests to these pages aren't linear
    search_result_pages = [num for num in range(0, 22700, 20)]
    random.shuffle(search_result_pages)
    for o in search_result_pages:
        search_results_page_attempt.append(o)
        try:
          search_results_loop(o)
        except Exception as ex:
          print(ex)
          search_results_page_error.append(o)
    print("Yay! We've finished scraping the whole site :). Now let's process the errors")
        #once we've finished looping through the initial search result pages, check the search_results_page_error for any entries
    while len(search_results_page_error) > 0:
        #We need to reset the ips list to make sure we're working with a clean set of proxies
        ips = []
        for x in search_results_page_error:
            try:
                search_results_page_attempt.append(x)
                search_results_loop(x)
                time_to_sleep = random.randint(5, 15)
                time.sleep(time_to_sleep)
                search_results_page_error.remove(x)
            except Exception as ex:
                print("There was another error with this SEARCH RESULTS PAGE. Moving on")
    # while len(profile_errors) > 0:
    #     ips = []
    #     get_proxy_list()
    #     p_count = 0
    #     for y in profile_errors:
    #         proxy = next(proxy_pool)
    #         proxies = {
    #             'http': proxy,
    #             'https': proxy
    #         }
    #         suffix = '/en/proboxer/' + y
    #         if check(proxy, 3) == True:
    #             try:
    #                 get_boxer_soup(proxies, suffix)
    #                 profile_errors.remove(y)
    #                 time_to_sleep = random.randint(5, 15)
    #                 p_count += 1
    #                 time.sleep(time_to_sleep)
    #                 if p_count > 200:
    #                     p_count = 0
    #                     get_proxy_list()
    #             except Exception as ex:
    #                 print('There was another error with this profile and/or bout list. Moving On.')
    #         else:
    #             print('Proxy Error when parsing through PROFILE ERRORS')
    while len(get_result_soup_errors) > 0:
        ips = []
        get_proxy_list()
        s_count = 0
        for z in get_result_soup_errors:
            proxy = next(proxy_pool)
            proxies = {
                'http': proxy,
                'https': proxy
            }
            if check(proxy, 3) == True:
                try:
                    get_boxer_soup(proxies, z)
                    get_result_soup_errors.remove(z)
                    time_to_sleep = random.randint(5, 15)
                    s_count += 1
                    time.sleep(time_to_sleep)
                    if s_count > 200:
                        s_count = 0
                        get_proxy_list()
                except Exception as ex:
                    print('There was another error processing this profile and/or bout list from get_result_soup_errors. Moving on.')
            else:
                print('Proxy Error when parsing through GET RESULT SOUP')
    print('All done!')


def check(proxy, timeout):
    check_url = 'https://httpbin.org/ip'
    check_proxies = {
      'http': proxy,
      'https': proxy
      }
    try:
        response = requests.get(check_url, proxies=check_proxies, timeout=timeout)
        if response.status_code == 200:
            return True
    except:
        return False


def get_proxy_list():
    r = requests.get('https://www.us-proxy.org/', headers=head).content
    soup = bs(r)
    for i in soup.find_all('table')[0].tbody.find_all('tr'):
        ip = i.find_all('td')[0].text
        port = i.find_all('td')[1].text
        ipport = str(ip) + ':' + str(port)
        ips.append(ipport)

def write_errors(lst):
    with open(r'C:\Users\Vicente\Documents\Projects\BoxRec_BS4\BeautifulSoup_Meets_BoxRec\profile_errors.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerows(lst)


get_proxy_list()
scrape_boxrec()
write_errors(profile_errors)
write_errors(bout_errors)
