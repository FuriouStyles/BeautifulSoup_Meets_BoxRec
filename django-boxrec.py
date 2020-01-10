from bs4 import BeautifulSoup
from itertools import cycle
import requests
import pandas as pd


ips = []
proxy_pool = cycle(ips)

def get_proxy_list():
    r = requests.get('https://www.us-proxy.org/', headers=head).content
    soup = bs(r)
    for i in soup.find_all('table')[0].tbody.find_all('tr'):
        ip = i.find_all('td')[0].text
        port = i.find_all('td')[1].text
        ipport = str(ip) + ':' + str(port)
        ips.append(ipport)

def get_working_proxy(proxy, timeout):
    check_url = 'https://httpbin.org/ip'
    check_proxies = {
      'http': proxy,
      'https': proxy
      }
    try:
        response = requests.get(check_url, proxies=check_proxies, timeout=timeout)
        if response.status_code == 200:
            return proxy
    except:
        next_proxy = next(proxy_pool)
        get_working_proxy(next_proxy, timeout)

def get_boxer_profile(url):
    head = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.70 Safari/537.36'}
    get_proxy_list()
    proxies = {
      'http': get_working_proxy(ips[0], 3),
      'https': get_working_proxy(ips[0], 3)
    }
    try:
        boxer_content = requests.get(url, headers=head, timeout=5, proxies=proxies).content
        boxer = bs(boxer_content, features='lxml')
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
        return profile
    except Exception as ex:
        return f"There was a problem with {name}'s profile: {ex}"

def get_boxer_soup(url, proxy, suffix):
    head = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.70 Safari/537.36'}
    boxer_content = requests.get(url, headers=head, timeout=5, proxies=proxy).content
    boxer = bs(boxer_content, features='lxml')
    get_boxer_profile(boxer, boxer_url)
