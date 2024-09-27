from pathlib import Path
import numpy as np
import pandas as pd
import duckdb


db_file = 'e_com_samples.duckdb'
duck = duckdb.connect(db_file)

# @st.cache_data(persist='disk')
def get_users():
    return users_rel

# @st.cache_data(persist='disk')
def get_categories():
    return categories_rel

def get_reviews2(selected_date):

    date_threshold = pd.to_datetime(selected_date) + pd.Timedelta(days = 1)
    reviews_df = (
        pd.read_parquet(
            'data/reviews.parquet',
            filters = [('review_timestamp', '<', date_threshold)]
        )
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

def get_top_rated(user, categories, selected_date):
    
    filter_on_user = ''
    if user != None:
        filter_on_user = f'and user_id != {user}'
    filter_on_categories = ''
    if len(categories)==1:
        filter_on_categories = f"where category = '{categories[0]}'"
    elif len(categories)>1:
        filter_on_categories = f"where category in {categories}"

    reviews_df = get_reviews(selected_date)

    top_items_query = f"""
        with
        selected_categories as (
            select category_id
            from categories
            {filter_on_categories}
        )
        , selected_items as (
            select item_id
            from selected_categories sc
                inner join items_info ii on sc.category_id = ii.category_id
        )
        , selected_reviews as (
            select *
            from reviews_df
            where
                item_id in (select item_id from selected_items)
                {filter_on_user}
        )
        , tops as (
            select
                item_id
                , avg(weighted_rating) as weighted_mean
            from selected_reviews
            group by item_id
        )
        select *
        from tops
            inner join items on tops.item_id = items.item_id
        order by weighted_mean desc
        limit 20
    """
    return duck.query(top_items_query).to_df()


users_rel = duck.query("select user_id from users")

categories_rel = duck.query("""
    select *
    from categories
    order by category
""")

reviews_rel = duck.query(f"""
        
    """)



