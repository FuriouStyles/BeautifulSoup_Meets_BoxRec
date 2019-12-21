import pandas as pd
import numpy as np
import datetime



def fights_build():
    boxers = pd.read_csv('https://raw.githubusercontent.com/FuriouStyles/BeautifulSoup_Meets_BoxRec/master/boxers.csv')
    bouts = pd.read_csv('https://raw.githubusercontent.com/FuriouStyles/BeautifulSoup_Meets_BoxRec/master/all_bouts.csv')

    fights = pd.merge(bouts, boxers, how='left', left_on='opponent_0_br_id', right_on='br_id')
    fights = pd.merge(fights.drop_duplicates(), boxers, how='left', left_on='opponent_br_id', right_on='br_id')

    return fights

def wrangle(df):
  # Drop columns and map merged column names to new ones
  df.drop(columns=['alias_y',
                  'alias_x',
                  'birth name_x',
                  'birth name_y',
                  'birth place_y',
                  'birth place_x',
                  'bouts_x',
                  'bouts_y',
                  'br_id_y',
                  'br_id_x',
                  'career_y',
                  'career_x',
                  'draws_y',
                  'draws_x',
                  'losses_y',
                  'losses_x',
                  'manager/agent_y',
                  'manager/agent_x',
                  'name_y',
                  'name_x',
                  'promoter_y',
                  'promoter_x',
                  'residence_y',
                  'residence_x',
                  'rounds_y',
                  'rounds_x',
                  'sex_x',
                  'status_y',
                  'status_x',
                  'titles held_y',
                  'titles held_x',
                  'wins_y',
                  'wins_x'],
            inplace=True)
  mapping = {
    'opponent': 'blue',
    'opponent_0': 'red',
    'opponent_0_br_id': 'red_br_id',
    'opponent_br_id': 'blue_br_id',
    'height': 'blue_height',
    'nationality': 'blue_nationality',
    'reach': 'blue_reach',
    'born_x': 'red_born',
    'debut_x': 'red_debut',
    'decision': 'red_decision',
    'stance_x': 'red_stance',
    'born_y': 'blue_born',
    'debut_y': 'blue_debut',
    'stance_y': 'blue_stance',
    'sex_y': 'sex',
    'division_x': 'red_division',
    'division_y': 'blue_division',
    'height_x': 'red_height',
    'height_y': 'blue_height',
    'reach_x': 'red_reach',
    'reach_y': 'blue_reach',
    'nationality_x': 'red_nationality',
    'nationality_y': 'blue_nationality',
    'w-l-d': 'blue_record_at_fight_time'
    }
  df.rename(columns=mapping, inplace=True)

  red_born = df['red_born'].str.split(pat=' / ', expand=True)
  df['red_born'] = red_born[0]
  df['red_age'] = red_born[1].str.split(expand=True)[1]

  blue_born = df['blue_born'].str.split(pat=' / ', expand=True)
  df['blue_born'] = blue_born[0]
  df['blue_age'] = blue_born[1].str.split(expand=True)[1]

  df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

  df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d', errors='coerce')
  df['red_born'] = pd.to_datetime(df['red_born'], format='%Y-%m-%d', errors='coerce')
  df['blue_born'] = pd.to_datetime(df['blue_born'], format='%Y-%m-%d', errors='coerce')
  df['red_debut'] = pd.to_datetime(df['red_debut'], format='%Y-%m-%d', errors='coerce')
  df['blue_debut'] = pd.to_datetime(df['blue_debut'], format='%Y-%m-%d', errors='coerce')

  df['red_height'] = df['red_height'].str.split(pat='/', expand=True)[1]
  df['blue_height'] = df['blue_height'].str.split(pat='/', expand=True)[1]

  df['red_reach'] = df['red_reach'].str.split(pat='/', expand=True)[1]
  df['blue_reach'] = df['blue_reach'].str.split(pat='/', expand=True)[1]

  df['red_age_at_fight_time'] = (df['date'] - df['red_born'])
  df['red_years_active'] = (df['date'] - df['red_debut'])
  df['blue_age_at_fight_time'] = (df['date'] - df['blue_born'])
  df['blue_years_active'] = (df['date'] - df['blue_debut'])

  df = df.drop_duplicates()

  df = df.dropna(subset=['date'])

  return df

# wrangle(fights)

def red_rec_to_blue(df):
    common = np.intersect1d(df['red_br_id'], df['blue_br_id'])
    common = pd.Series(common)
    df = df[df['red_br_id'].isin(common)]
    for i in df['date']:
        grouped = df.groupby('date').get_group(i)
        df['red_record_at_fight_time'] = grouped['red_br_id'].apply(lambda x: grouped.loc[grouped["blue_br_id"] == x, "blue_record_at_fight_time"].values)

    return df

fights = fights_build()
fights = wrangle(fights)
fights = red_rec_to_blue(fights)

fights.to_csv(r'C:\Users\Vicente\Documents\Projects\BoxRec_BS4\BeautifulSoup_Meets_BoxRec\fights.csv', index=False, mode='w', na_rep='NaN')
