from datetime import datetime as dtime
from pathlib import Path
import numpy as np
import pandas as pd
import streamlit as st
import etl
import user_based

min_date = dtime(1998, 3, 16)
max_date = dtime(2023, 9, 12)

st.set_page_config(layout = 'wide')


def populate_columns(df, columns_cnt):
    columns = st.columns(columns_cnt, gap='medium')
    items = df['item_id'].values.tolist()
    for c, id in zip(columns, items):
        item_row = df.query(f"item_id=={id}")
        with c:
            st.image(item_row['image'].values[0])
            st.write(item_row['title'].values[0][:50])    


# название сервиса
st.title('E-com')

#########################################################################################
#########################################################################################
# ФИЛЬТРЫ
#########################################################################################
#########################################################################################
col1, col2, col3 = st.columns(3)

#----------------------------------------------------------------------------------------
# фильтр даты
#----------------------------------------------------------------------------------------
with col1:
    selected_date = st.date_input(
        label = 'select date',
        value = pd.to_datetime('2019-12-31'),
        min_value = min_date,
        max_value = max_date
    )

reviews_df = etl.get_reviews2(selected_date)

#----------------------------------------------------------------------------------------
# фильтр категорий
#----------------------------------------------------------------------------------------
categories_lst = reviews_df['category'].sort_values().unique().tolist()
with col2:
    categories = st.multiselect(
        label = 'select categories',
        options = categories_lst
    )

filtered_df = reviews_df.query(f"category {'not' if len(categories)==0 else ''} in {categories}")

#----------------------------------------------------------------------------------------
# фильтр пользователей
#----------------------------------------------------------------------------------------
users = filtered_df['user_id'].sort_values().unique().tolist()
items_df = pd.read_parquet(
    'data/items.parquet',
    filters=[('item_id', 'in', filtered_df['item_id'].unique().tolist())]
)

# st.write(categories)
# # st.write(users)
st.dataframe(filtered_df)

with col3:
    user = st.selectbox(
        label = 'select user',
        options = users, 
        index = None
    )

#########################################################################################
#########################################################################################
# ВКЛАДКИ
#########################################################################################
#########################################################################################

tab1, tab2, tab3 = st.tabs(['popular products', 'user-based recommendations', 'similar products'])


#----------------------------------------------------------------------------------------
# вкладка POPULAR PRODUCTS
#----------------------------------------------------------------------------------------
with tab1:
    # st.write(etl.get_top_rated(user, tuple(categories), selected_date))
    # top_rated = etl.get_top_rated(user, tuple(categories), selected_date)
    top_rated = etl.get_top_rated(user, filtered_df, items_df)

    populate_columns(top_rated.iloc[:5], 5)
    populate_columns(top_rated.iloc[5:10], 5)
    populate_columns(top_rated.iloc[10:15], 5)
    populate_columns(top_rated.iloc[15:20], 5)


#----------------------------------------------------------------------------------------
# вкладка USER-BASED RECOMMENDATIONS
#----------------------------------------------------------------------------------------
with tab2:

    # go = st.button(label = 'recommend')
    
    # if go:
    st.write(categories)

    # ratings_df = filtered_df.pivot_table(
    #     index = 'user_id',
    #     columns = 'item_id',
    #     values = 'rating'
    # )

    # avg_ratings = ratings_df.mean(axis = 1)

    # st.write(type(avg_ratings))

    if user == None:
        st.title("select a user")
    else:
        recommended_items = user_based.recommend(user, filtered_df)
    
        if len(recommended_items) > 0:
            st.dataframe(recommended_items)
            st.write(type(recommended_items))
        else:
            st.title("no similar users found")

#----------------------------------------------------------------------------------------
# вкладка SIMILAR PRODUCTS
#----------------------------------------------------------------------------------------



