from pymongo import MongoClient
import pandas as pd
from functions import *
import pickle
import xgboost as xgb
import streamlit as st



with open('forward_pipeline.pkl', 'rb') as file:
    forward_pipeline = pickle.load(file)

with open('forward_model.pkl', 'rb') as file:
    forward_model = pickle.load(file)

with open('midfield_pipeline.pkl', 'rb') as file:
    midfield_pipeline = pickle.load(file)

with open('midfield_model.pkl', 'rb') as file:
    midfield_model = pickle.load(file)

with open('defender_pipeline.pkl', 'rb') as file:
    defender_pipeline = pickle.load(file)

with open('defender_model.pkl', 'rb') as file:
    defender_model = pickle.load(file)

with open('gk_pipeline.pkl', 'rb') as file:
    gk_pipeline = pickle.load(file)

with open('goalkeeper_model.pkl', 'rb') as file:
    gk_model = pickle.load(file)



client = MongoClient('mongodb+srv://tanitoluwaadebayo:VRZzIOdFQbbRsdzS@cluster0.5tl5idr.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0', 
ssl=True, ssl_cert_reqs=None)
db = client['Cluster0']
collection = db['FPL_Data']
print('Connected to database!')



st.title('Fantasy Premier League Points Forecaster')
st.write("**Enter a players name as specified on the fpl website**")
if 'home_opponent_team_inputs' not in st.session_state:
    st.session_state.home_opponent_team_inputs = False
if 'table' not in st.session_state:
    st.session_state.table = False

name_input = st.text_input("Enter the player name")

col1, col2, col3, col4, col5 = st.columns(5)
table = None


with col1:
    next_gw = st.button("Forecast next game week")
    if next_gw:
        st.session_state.home_opponent_team_inputs = True
with col2:
    top_ten_forwards = st.button("Get top 10 Forwards")
    if top_ten_forwards:
        st.session_state.table = False
        table = pd.DataFrame(get_top_10_forwards(collection, forward_model, forward_pipeline))
        st.session_state.table = True
with col3:
    top_ten_midfielders = st.button("Get top ten midfielders")
    if top_ten_midfielders:
        st.session_state.table = False
        table = pd.DataFrame(get_top_10_midfielders(collection, midfield_model, midfield_pipeline))
        st.session_state.table = True

with col4:
    top_ten_defenders = st.button("Get top ten defenders")
    if top_ten_defenders:
        st.session_state.table = False
        table = pd.DataFrame(get_top_10_defenders(collection, defender_model, defender_pipeline))
        st.session_state.table = True

with col5:
    top_ten_gk = st.button("Get top ten goalkeepers")
    if top_ten_gk:
        st.session_state.table = False
        table = pd.DataFrame(get_top_10_goalkeepers(collection, gk_model, gk_pipeline))
        st.session_state.table = True


if st.session_state.home_opponent_team_inputs == True:
    col6, col7, col8 = st.columns(3)
    with col6:
        st.write("Check if the player is home in the next fixture")
        is_home = st.checkbox("Is player home?")
    with col7:
        st.write("Opponent team strength")
        op_team = st.selectbox('Choose opponent strength', ['top 6', 'mid table', 'relegation fodder'])
    with col8:
        predict_button = st.button('Predict')
        if predict_button:
            data = get_latest_player_stats(collection, name_input.strip())
            if data['position'] == 'FWD':
                fixture_data = {'team_category': op_team, 'was_home': is_home}
                prediction = predict(data, forward_pipeline, forward_model, fixture_data)
                print(data)
                print(prediction)
                st.write(prediction[0])
            elif data['position'] == 'MID':
                fixture_data = {'team_category': op_team, 'was_home': is_home}
                prediction = predict(data, midfield_pipeline, midfield_model, fixture_data)
                print(data)
                print(prediction)
                st.write(prediction[0])
            elif data['position'] == 'DEF':
                fixture_data = {'team_category': op_team, 'was_home': is_home}
                prediction = predict(data, forward_pipeline, forward_model, fixture_data)
                print(data)
                print(prediction)
                st.write(prediction[0])
            else:
                fixture_data = {'team_category': op_team, 'was_home': is_home}
                prediction = predict(data, forward_pipeline, forward_model, fixture_data)
                print(data)
                print(prediction)
                st.write(prediction[0])

if st.session_state.table:
    st.write(table)





