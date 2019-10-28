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
boxers = pd.DataFrame(columns=['status', 'career', 'titles held', 'birth name', 'alias', 'born', 'nationality', 'debut', 'division', 'stance', 'height', 'reach', 'residence', 'birth place', 'manager/agent', 'name', 'wins', 'losses', 'draws' ])
all_bouts = pd.DataFrame(columns=['date', 'opponent', 'w-l-d', 'venue', 'result', 'decision', 'opponent_0'])
head = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.70 Safari/537.36'}
ips = []
proxy_pool = cycle(ips)
search_results_page_attempt = []
search_results_page_error = []
profile_errors = set()
bout_errors = set()

def get_boxer_profile(boxer, url):
  # boxer is the variable that we're going to pass in that is the soup of each boxer's whole profile page, as we'be done with Lomachenko at the top of the notebook
  # This will pull the data that we're interested in from the profile table of each boxer
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
    #print(f"Appending {name} to the Boxers DataFrame")
    global boxers
    boxers = boxers.append(profile, ignore_index=True) # This will append the boxer's profile to the main boxers dataframe that contains all of the boxer profiles we scrape
  except Exception as ex:
    #print(f"There was a problem with {name}'s profile: {ex}")
    profile_errors.add(br_id)

def get_bouts(boxer, url):
  try:
    name = boxer.table.h1.text
    br_id = boxer.table.h2.text
    br_id = br_id.strip('ID# ')
    tables = boxer.find_all('table')
    profile = str(tables)
    bouts = pd.read_html(profile)
    bouts = bouts[8]
    search_for = ['Title', 'google', 'scheduled']
    bouts = bouts[~bouts['w-l-d'].str.contains('|'.join(search_for), na=False)]
    bouts.drop(columns=['last 6', 'Unnamed: 0', 'Unnamed: 2', 'Unnamed: 9', 'Unnamed: 10'], inplace=True)
    bouts.rename(columns={'result': 'decision'}, inplace=True)
    bouts.rename(columns={'Unnamed: 8': 'result', 'Unnamed: 6': 'venue'}, inplace=True)
    bouts['opponent_0'] = name
    #print(f"Appending bout list for {name} to the All Bouts DataFrame")
    global all_bouts
    all_bouts = all_bouts.append(bouts, ignore_index=True) # Appends the boxers bouts to the all_bouts dataframe
  except Exception as ex:
    #print(f"There was an exception with {name}'s bout sheet: {ex}")
    bout_errors.add(br_id)

def get_result_soup(soup, proxies):
  #print("Current location: get_result_soup")
  results_link_soup = soup.select('.personLink')
  results_list = [i['href'] for i in results_link_soup]
  for i in results_list:
    boxer_url = 'https://boxrec.com' + i
    boxer_content = requests.get(boxer_url, headers=head, timeout=5, proxies=proxies).content
    boxer = bs(boxer_content, features='lxml')
    get_boxer_profile(boxer, boxer_url)
    get_bouts(boxer, boxer_url)
    sleeper = random.randint(3, 5)
    time.sleep(sleeper)

def scrape_boxrec():
  search_result_pages = [num for num in range(0, 22700, 20)]
  random.shuffle(search_result_pages)
  for i in search_result_pages:
    get_proxy_list()
    search_results_page_attempt.append(i)
    proxy = next(proxy_pool)
    proxies = {
        'http': proxy,
        'https': proxy
    }
    if check(proxy, 3) == True:
      url = 'https://boxrec.com/en/locations/people?l%5Brole%5D=proboxer&l%5Bdivision%5D=&l%5Bcountry%5D=&l%5Bregion%5D=&l%5Btown%5D=&l_go=&offset=' + str(i)
      try:
        results_source = requests.get(url, headers=head, timeout=5, proxies=proxies)
        #print(f"Status: {results_source.status_code}. Good to go.")
        results_content = results_source.text
        res_soup = bs(results_content, 'lxml')
        get_result_soup(res_soup, proxies)
        time_to_sleep = random.randint(5, 15)
        #print(f"Sleeping for {time_to_sleep} seconds.")
        time.sleep(time_to_sleep)
      except Exception as ex:
        search_results_page_error.append(i)
        print(ex)
    else:
      print("Bad Proxy. Moving On")
      search_results_page_error.append(i)
  if len(search_results_page_error) > 0:
    ips = []
    get_proxy_list()
    for x in search_results_page_error:
      search_results_page_attempt.append(x)
      proxy = next(proxy_pool)
      proxies = {
          'http': proxy,
          'https': proxy
      }
      if check(proxy, 5) == True:
        url = 'https://boxrec.com/en/locations/people?l%5Brole%5D=proboxer&l%5Bdivision%5D=&l%5Bcountry%5D=&l%5Bregion%5D=&l%5Btown%5D=&l_go=&offset=' + str(x)
        try:
          results_source = requests.get(url, headers=head, timeout=5, proxies=proxies)
          print(f"Status: {results_source.status_code}. Good to go.")
          results_content = results_source.text
          res_soup = bs(results_content, 'lxml')
          get_result_soup(res_soup, proxies)
          time_to_sleep = random.randint(5, 15)
          print(f"Sleeping for {time_to_sleep} seconds.")
          time.sleep(time_to_sleep)
          search_results_error.pop(x)
        except Exception as ex:
          search_results_page_error.append(x)
          print(ex)
      else:
        print("Bad Proxy. Moving On")
        search_results_page_error.append(x)

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

get_proxy_list()
scrape_boxrec()
