import pandas as pd
from pymongo import MongoClient

df1 = pd.read_csv('C:/Users/User/Desktop/AI-Final-Project-/merged_gw_23-34.csv')
df1['season'] = 0
df2 = pd.read_csv('C:/Users/User/Desktop/AI-Final-Project-/merged_gw_2022-23.csv')
df2['season'] = 1

df = pd.concat([df1, df2]).reset_index(drop = True)

df = df.sort_values(by=['name','season', 'GW']).reset_index(drop =True)

window_size = 38
df[f'avg_points_last_{window_size}'] = df.groupby(['name'])['total_points'].transform(lambda x: x.rolling(window=window_size, min_periods=1).mean().shift(1))
df[f'avg_minutes_last_{window_size}'] = df.groupby(['name'])['minutes'].transform(lambda x: x.rolling(window=window_size, min_periods = 1).mean().shift(1))
df[f'avg_ict_index_last_{window_size}'] = df.groupby(['name'])['ict_index'].transform(lambda x: x.rolling(window=window_size, min_periods = 1).mean().shift(1))
df[f'avg_bonus_last_{window_size}'] = df.groupby(['name'])['bonus'].transform(lambda x: x.rolling(window=window_size, min_periods = 1).mean().shift(1))
df[f'avg_bps_last_{window_size}'] = df.groupby(['name'])['bps'].transform(lambda x: x.rolling(window=window_size, min_periods = 1).mean().shift(1))
df[f'avg_xP_last_{window_size}'] = df.groupby(['name'])['xP'].transform(lambda x: x.rolling(window = window_size, min_periods = 1).mean().shift(1))
df[f'avg_expected_goal_involvements_last_{window_size}'] = df.groupby(['name'])['expected_goal_involvements'].transform(lambda x: x.rolling(window = window_size, min_periods = 1).mean().shift(1))
df[f'avg_expected_goals_conceded_last_{window_size}'] = df.groupby(['name'])['expected_goals_conceded'].transform(lambda x: x.rolling(window = window_size, min_periods = 1).mean().shift(1))
df[f'avg_goals_conceded_last_{window_size}'] = df.groupby(['name'])['goals_conceded'].transform(lambda x: x.rolling(window = window_size, min_periods = 1).mean().shift(1))


df[f'avg_goals_scored_last_{window_size}'] = df.groupby(['name'])['goals_scored'].transform(lambda x: x.rolling(window = window_size, min_periods = 1).mean().shift(1))
df[f'avg_influence_last_{window_size}'] = df.groupby(['name'])['influence'].transform(lambda x: x.rolling(window = window_size, min_periods = 1).mean().shift(1))
df[f'avg_threat_last_{window_size}'] = df.groupby(['name'])['threat'].transform(lambda x: x.rolling(window = window_size, min_periods = 1).mean().shift(1))
df[f'avg_creativity_last_{window_size}'] = df.groupby(['name'])['creativity'].transform(lambda x: x.rolling(window = window_size, min_periods = 1).mean().shift(1))

bins = [0, 6, 14, 20]
labels = ['top 6', 'mid table', 'relegation fodder']

df['team_category'] = pd.cut(df['opponent_team'], bins=bins, labels=labels, right=True)

useless_columns = [
    'team', 'xP', 'assists', 'bonus', 'bps', 'clean_sheets', 'creativity', 'element', 'expected_assists',
    'expected_goal_involvements', 'expected_goals', 'expected_goals_conceded', 'fixture',  'goals_conceded', 'goals_scored',
       'ict_index', 'influence', 'kickoff_time', 'minutes', 'own_goals', 'penalties_missed', 'penalties_saved', 'red_cards',
       'round', 'saves', 'selected', 'starts', 'team_a_score', 'team_h_score','threat', 'transfers_balance', 'transfers_in',
       'transfers_out', 'yellow_cards', 'GW'
]

df.drop(columns = useless_columns, inplace = True)

df[f'avg_points_last_{window_size}'] = df[f'avg_points_last_{window_size}'].fillna(0)
df[f'avg_minutes_last_{window_size}'] = df[f'avg_minutes_last_{window_size}'].fillna(0)
df[f'avg_ict_index_last_{window_size}'] = df[f'avg_ict_index_last_{window_size}'].fillna(0)
df[f'avg_bonus_last_{window_size}'] = df[f'avg_bonus_last_{window_size}'].fillna(0)
df[f'avg_bps_last_{window_size}'] = df[f'avg_bps_last_{window_size}'].fillna(0)
df[f'avg_xP_last_{window_size}'] = df[f'avg_xP_last_{window_size}'].fillna(0)
df[f'avg_expected_goal_involvements_last_{window_size}'] = df[f'avg_expected_goal_involvements_last_{window_size}'].fillna(0)
df[f'avg_expected_goals_conceded_last_{window_size}'] = df[f'avg_expected_goals_conceded_last_{window_size}'].fillna(0)
df[f'avg_goals_conceded_last_{window_size}'] = df[f'avg_goals_conceded_last_{window_size}'].fillna(0)

df[f'avg_goals_scored_last_{window_size}'] = df[f'avg_goals_scored_last_{window_size}'].fillna(0)
df[f'avg_influence_last_{window_size}'] = df[f'avg_influence_last_{window_size}'].fillna(0)
df[f'avg_threat_last_{window_size}'] = df[f'avg_threat_last_{window_size}'].fillna(0)
df[f'avg_creativity_last_{window_size}'] = df[f'avg_creativity_last_{window_size}'].fillna(0)

def write_to_db():
    client = MongoClient('mongodb+srv://tanitoluwaadebayo:VRZzIOdFQbbRsdzS@cluster0.5tl5idr.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
    db = client['Cluster0']
    print('Connected')
    collection = db['FPL_Data']
    data_dict = df.to_dict(orient='records')
    collection.insert_many(data_dict)
    print('inserted_data')

write_to_db()