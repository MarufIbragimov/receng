from pathlib import Path
import numpy as np
import pandas as pd
import duckdb


db_file = 'e_com_samples0005.duckdb'
duck = duckdb.connect(db_file)

# @st.cache_data(persist='disk')
def get_users(selected_date):
    date_threshold = pd.to_datetime(selected_date) + pd.Timedelta(days = 1)
    users = pd.read_parquet(
            'data/reviews.parquet',
            filters = [('review_timestamp', '<', date_threshold)]
        ).sort_values(by='user_id')['user_id'].unique().tolist()

    return users

# @st.cache_data(persist='disk')
def get_categories():
    

    categories_rel = duck.query("""
        select *
        from categories
        order by category
    """)
    return categories_rel

def get_reviews2(selected_date, direction):

    date_threshold = pd.to_datetime(selected_date) + pd.Timedelta(days = 1)
    
    reviews_df = (
        pd.read_parquet(
            'data/reviews_data.parquet',
            filters = [('review_timestamp', direction, date_threshold)]#, ('category_id', 'in', categories)]
        )
        # .query(f"category {'not' if len(categories)<1  else ''} in {categories}")
        .sort_values(by=['review_timestamp'])
        .drop_duplicates(subset = ['user_id', 'item_id'], keep='last')
        .assign(
            helpful_vote = lambda d_: np.where(d_['helpful_vote']==0, d_['helpful_vote'] + 1, d_['helpful_vote']),
            verified_purchase = lambda d_: np.where(d_['verified_purchase'], 2, 1),
            weighted_rating = lambda d_: d_['rating'] * d_['helpful_vote'] * d_['verified_purchase']
        )
    )

    return reviews_df


def get_reviews(selected_date):
    reviews_df = duck.query("""
        with 
        reviews_ as (
            select
                user_id
                , item_id
                , review_timestamp
                , rating
                , case
                    when helpful_vote=0 then helpful_vote+1
                    else helpful_vote
                end as helpful_vote
                , case 
                    when verified_purchase then 2
                    else 1
                end as verified_purchase
            from reviews
        )
        select
            r.*
            , r.rating * r.helpful_vote * r.verified_purchase as weighted_rating
        from reviews_ r
        order by review_timestamp
    """).to_df()

    reviews_df = (
        reviews_df[reviews_df['review_timestamp'].dt.date<=pd.to_datetime(selected_date).date()]
        .drop_duplicates(subset = ['user_id', 'item_id'], keep='last')
    )

    return reviews_df



def test_reviews(selected_date):
    reviews_df = get_reviews(selected_date)

    return duck.query("select * from reviews_df")




def show_table(tbl):
    return duck.query(f"select * from {tbl}")


def get_top_rated(user, reviews, items):
        
    tops_df = (
        reviews.query(f"user_id!={user}") #{'!=' if user != None else '=='}
        .groupby(['item_id'], as_index=False).agg(weighted_mean = ('weighted_rating', 'mean'))
        .sort_values(by=['weighted_mean'], ascending=False)
        .head(20)
        .merge(items, how='left', on='item_id')
    )
    return tops_df






