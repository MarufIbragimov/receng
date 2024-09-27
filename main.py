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


st.title('E-com')

# with st.sidebar:
col1, col2, col3 = st.columns(3)

with col1:
    selected_date = st.date_input(
        label = 'select date',
        value = pd.to_datetime('2019-12-31'),
        min_value = min_date,
        max_value = max_date
    )

with col2:
    user = st.selectbox(
        label = 'select user',
        options = etl.get_users().to_df()['user_id'],
        index = None
    )

with col3:
    categories = st.multiselect(
        label = 'select categories',
        options = etl.get_categories().to_df()['category']
    )

    # go = st.button('Go')


# st.write(
#     etl.test_reviews(selected_date).to_df()#.aggregate("user_id, item_id, count(*) as cnt").order("cnt desc")
    
# )
# st.dataframe(
#     etl.show_table('items_info')
# )


# Custom CSS to inject

st.markdown("""
<style>

	.stTabs [data-baseweb="tab-list"] {
		gap: 20px;
    }

	.stTabs [data-baseweb="tab"] {
		height: 50px;
        white-space: pre-wrap;
		#background-color: #F0F2F6;
		border-radius: 4px 4px 0px 0px;
		gap: 10px;
		padding-top: 10px;
		padding-bottom: 10px;
    }

	# .stTabs [aria-selected="true"] {
  	# 	background-color: #FFFFFF;
	# }

</style>""", unsafe_allow_html=True)

tab1, tab2, tab3 = st.tabs(['popular products', 'personal recommendations', 'similar products'])

# if go:        
    # st.write(min_date, max_date)
    # st.header('Top 10 high-rated products')
    # st.write(user)
    # st.write(categories)


with tab1:
    # st.write(etl.get_top_rated(user, tuple(categories), selected_date))
    top_rated = etl.get_top_rated(user, tuple(categories), selected_date)

    populate_columns(top_rated.iloc[:5], 5)
    populate_columns(top_rated.iloc[5:10], 5)
    populate_columns(top_rated.iloc[10:15], 5)
    populate_columns(top_rated.iloc[15:20], 5)

with tab2:
    st.dataframe(user_based.ratings_matrice(selected_date))