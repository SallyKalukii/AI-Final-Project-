import pandas as pd

feature_names = ['value', 'avg_points_last_38', 'avg_minutes_last_38',
       'avg_ict_index_last_38', 'avg_bonus_last_38', 'avg_bps_last_38',
       'avg_xP_last_38', 'avg_expected_goal_involvements_last_38',
       'avg_expected_goals_conceded_last_38', 'avg_goals_conceded_last_38',
       'avg_goals_scored_last_38', 'avg_influence_last_38',
       'avg_threat_last_38', 'avg_creativity_last_38', 'was_home_True',
       'team_category_relegation fodder', 'team_category_top 6']

to_be_dropped = ['_id', 'name', 'position', 'opponent_team', 'total_points', 'season']

def get_latest_player_stats(collection, name):
    document = collection.aggregate([
    { '$match': { 'name': name } },
    { '$sort': { '_id': -1 } },
    { '$limit': 1 }
    ])
    document = list(document)
    return document[-1] if len(document) > 0 else []

def predict(data, pipeline, model, fixture_data):
    data = pd.DataFrame.from_dict({0: data}, orient='index')
    data.drop(columns = to_be_dropped, inplace = True)
    data['was_home'] = fixture_data['was_home']
    data['team_category'] = fixture_data['team_category']

    
    data = pd.DataFrame(pipeline.transform(data), columns = feature_names)
    return model.predict(data)


#[6.65293491]

def get_top_10_forwards(collection, forward_model, forward_pipeline):
   
    df = pd.read_csv('C:/Users/User/Desktop/AI-Final-Project-/cleaned_players.csv')
    df['name'] = df['first_name']+' '+df['second_name']
    df = df[df['element_type'] == 'FWD']
    points = []
    for name in df['name']:
        ans = {'name': name}
        data = get_latest_player_stats(collection, name)
        if len(data) == 0:
            continue
        point = predict(data, forward_pipeline, forward_model, 
                         {'was_home': True, 'team_category':'top 6'})
        ans['point'] = point
        points.append(ans)
    points = sorted(points, key= lambda x: x['point'], reverse = True)
    print(points[:10])
    return points[:10]


def get_top_10_midfielders(collection, midfielder_model, midfielder_pipeline):
    df = pd.read_csv('C:/Users/User/Desktop/AI-Final-Project-/cleaned_players.csv')
    df['name'] = df['first_name']+' '+df['second_name']
    df = df[df['element_type'] == 'MID']
    points = []
    for name in df['name']:
        ans = {'name': name}
        data = get_latest_player_stats(collection, name)
        if len(data) == 0:
            continue
        point = predict(data, midfielder_pipeline, midfielder_model, 
                         {'was_home': True, 'team_category':'top 6'})
        ans['point'] = point
        points.append(ans)
    points = sorted(points, key= lambda x: x['point'], reverse = True)
    print(points[:10])
    return points[:10]

def get_top_10_defenders(collection, defender_model, defender_pipeline):
    df = pd.read_csv('C:/Users/User/Desktop/AI-Final-Project-/cleaned_players.csv')
    df['name'] = df['first_name']+' '+df['second_name']
    df = df[df['element_type'] == 'DEF']
    points = []
    for name in df['name']:
        ans = {'name': name}
        data = get_latest_player_stats(collection, name)
        if len(data) == 0:
            continue
        point = predict(data, defender_pipeline, defender_model, 
                         {'was_home': True, 'team_category':'top 6'})
        ans['point'] = point
        points.append(ans)
    points = sorted(points, key= lambda x: x['point'], reverse = True)
    print(points[:10])
    return points[:10]

def get_top_10_goalkeepers(collection, goalkeeper_model, goalkeeper_pipeline):
    df = pd.read_csv('C:/Users/User/Desktop/AI-Final-Project-/cleaned_players.csv')
    df['name'] = df['first_name']+' '+df['second_name']
    df = df[df['element_type'] == 'GK']
    points = []
    for name in df['name']:
        ans = {'name':name}
        data = get_latest_player_stats(collection, name)
        if len(data) == 0:
            continue
        point = predict(data, goalkeeper_pipeline, goalkeeper_model, 
                         {'was_home': True, 'team_category':'top 6'})
        ans['point'] = point
        points.append(ans)
    points = sorted(points, key= lambda x: x['point'], reverse = True)
    print(points[:10])
    return points[:10]

