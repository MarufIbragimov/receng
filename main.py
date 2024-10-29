from datetime import datetime as dtime
from pathlib import Path
import numpy as np
import pandas as pd
import streamlit as st
import etl
import user_based
import item_based

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

reviews_df = etl.get_reviews2(selected_date, '<=')

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


with col3:
    user = st.selectbox(
        label = 'select user',
        options = users, 
        index = None
    )

items_df = pd.read_parquet(
    'data/items.parquet',
    filters=[('item_id', 'in', filtered_df['item_id'].unique().tolist())]
)

# st.write(categories)
# # st.write(users)


#########################################################################################
#########################################################################################
# ПРОШЛЫЕ И БУДУЩИЕ ПРОДУКТЫ ВЫБРАННЫЕ ПОЛЬЗОВАТЕЛЕМ
#########################################################################################
#########################################################################################


st.header("User items")

col1, col2 = st.columns(2)

with col1:
    st.subheader("past items")

    user_reviews = filtered_df.query(f"user_id == {user}")
    user_choices = filtered_df.query(f"user_id == {user}")['item_id'].values.tolist()
    user_ranked = items_df.query(f"item_id in {user_choices}").head(20)

    populate_columns(user_ranked.iloc[:5], 5)
    populate_columns(user_ranked.iloc[5:10], 5)
    populate_columns(user_ranked.iloc[10:15], 5)
    populate_columns(user_ranked.iloc[15:20], 5)

    st.dataframe(user_reviews[['user_id', 'item_id', 'review_timestamp', 'rating']])

with col2:
    st.subheader("future items")

    future_reviews = (
        etl.get_reviews2(selected_date=selected_date, direction='>')
        .query(f"user_id == {user}")
    )[['user_id', 'item_id', 'review_timestamp', 'rating']]

    future_items = pd.read_parquet(
        'data/items.parquet',
        columns = ['item_id', 'title', 'image'],
        filters=[('item_id', 'in', future_reviews['item_id'].unique().tolist())]
    ).head(20)

    items_to_be_ranked = future_reviews.merge(
        right = future_items,
        how = 'inner',
        on = 'item_id'
    ).reset_index(drop=True)

    populate_columns(items_to_be_ranked.iloc[:5], 5)
    populate_columns(items_to_be_ranked.iloc[5:10], 5)
    populate_columns(items_to_be_ranked.iloc[10:15], 5)
    populate_columns(items_to_be_ranked.iloc[15:20], 5)

    st.dataframe(items_to_be_ranked[['user_id', 'item_id', 'review_timestamp', 'rating']])




#########################################################################################
#########################################################################################
# ВКЛАДКИ
#########################################################################################
#########################################################################################

tab1, tab2, tab3 = st.tabs(['Popular products', 'User-based recommendations', 'Item-based recommendations'])


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

    if user == None:
        st.title("please, select a user")
    else:
        recommended_items = user_based.recommend(user, filtered_df)
    
        # st.write(recommended_items)#.to_frame())

        if len(recommended_items) > 0:
            items_to_recommend = (
                recommended_items.to_frame()
                .merge(
                    right = items_df,
                    how = 'inner',
                    left_index = True,
                    right_on = 'item_id'
                )
                .reset_index(drop=True)
            )

            populate_columns(items_to_recommend.iloc[:5], 5)
            populate_columns(items_to_recommend.iloc[5:10], 5)
            populate_columns(items_to_recommend.iloc[10:15], 5)
            populate_columns(items_to_recommend.iloc[15:20], 5)
        
        else:
            st.title("no similar users found")

#----------------------------------------------------------------------------------------
# вкладка SIMILAR PRODUCTS
#----------------------------------------------------------------------------------------

with tab3:
    
    if user == None:
        st.title("please, select a user")
    else: 

        items_based_recommendation = item_based.recommend(user, filtered_df)
        items_to_recommend = (
            items_based_recommendation
            .merge(
                right = items_df,
                how = 'inner',
                left_index = True,
                right_on = 'item_id'
            )
            .reset_index(drop=True)
        )

        # st.dataframe(items_to_recommend)

        populate_columns(items_to_recommend.iloc[:5], 5)
        populate_columns(items_to_recommend.iloc[5:10], 5)
        populate_columns(items_to_recommend.iloc[10:15], 5)
        populate_columns(items_to_recommend.iloc[15:20], 5)
