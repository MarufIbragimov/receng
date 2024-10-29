from pathlib import Path
import numpy as np
import pandas as pd
import duckdb
import etl
from sklearn.metrics.pairwise import cosine_similarity
import streamlit as st
from scipy.sparse.linalg import svds


from sklearn.metrics import jaccard_score
from scipy.spatial.distance import pdist, squareform



def center_ratings(df: pd.DataFrame, averages: pd.Series) -> pd.DataFrame:
    """
    Возвращает рейтинги, где каждый рейтинг присвоенный пользователем скорректирован на среднее значение всех рейтингов,
    присвоенных этим абонентом каким-либо продуктам.

    Принимает:
    * df - датафрейм с рейтингами, где в индексе id пользователей а в столбцах id продуктов.
    * averages - серия со средним рейтингом каждого пользователя

    Возвращает:
    * centered_df - датафрейм, содержащий рейтинги, центрированные по среднему значению рейтингов каждого пользователя
    """
    centered_df = df.copy().sub(averages, axis = 0).fillna(0)
    return centered_df


def refactorize(df: pd.DataFrame) -> pd.DataFrame:
    # u, sigma, vt = np.linalg.svd(df.values, full_matrices=False)
    u, sigma, vt = svds(df.values)
    
    sigma_diag = np.diag(sigma)

    u_sigma = np.dot(u, sigma_diag)
    u_sigma_vt = np.dot(u_sigma, vt)

    recalculated_df = pd.DataFrame(
        data = u, #u_sigma_vt,
        index = df.index,
        #columns =  df.columns
    )

    return recalculated_df


def recommend(user: int, filtered_df: pd.DataFrame):
    
    ratings_df = filtered_df.pivot_table(
        index = 'user_id',
        columns = 'item_id',
        values = 'rating'
    )

    avg_ratings = ratings_df.mean(axis = 1)

    ratings_centered_df = center_ratings(ratings_df, avg_ratings)

    recalculated_ratings_df = refactorize(ratings_centered_df) 

    jaccard_distances = pdist(recalculated_ratings_df.values, metric='jaccard')
    square_jaccard_dists = squareform(jaccard_distances)
    jaccard_similarity_arr = 1- square_jaccard_dists

    all_similarities_df = pd.DataFrame(
        jaccard_similarity_arr,
        index = recalculated_ratings_df.index,
        columns = recalculated_ratings_df.index
    )


    similar_users_df = (
        all_similarities_df
        .query(f"user_id!={user}")[user]
        .sort_values(ascending=False)
    )

    user_items = ratings_df.query(f"user_id == {user}").T.dropna().index.tolist()    
    other_items_df = ratings_centered_df.query(f"user_id !={user}").T.query(f"~item_id.isin({user_items})").T
    
    recommended_items = avg_ratings.loc[user] + (other_items_df * similar_users_df).sum() / similar_users_df.sum()
    recommended_items.sort_values(inplace=True)

    return recommended_items.head(20) 






# def recommend2(user: int, filtered_df: pd.DataFrame):
    
#     ratings_df = filtered_df.pivot_table(
#         index = 'user_id',
#         columns = 'item_id',
#         values = 'rating'
#     )

#     avg_ratings = ratings_df.mean(axis = 1)

#     ratings_centered_df = center_ratings(ratings_df, avg_ratings)

#     recalculated_ratings_df = refactorize(ratings_centered_df) 

#     all_similarities_df = pd.DataFrame(
#         data = cosine_similarity(recalculated_ratings_df),
#         index = recalculated_ratings_df.index,
#         columns = recalculated_ratings_df.index
#     )

#     user_items = ratings_df.query(f"user_id == {user}").T.dropna().index.tolist()    
#     other_items_df = recalculated_ratings_df.query(f"user_id !={user}").T.query(f"~item_id.isin({user_items})").T

#     user_similarities = all_similarities_df.query(f"user_id!={user}")[user]
    
#     recommended_items = avg_ratings.loc[user] + (other_items_df * user_similarities).sum() / user_similarities.sum()

#     recommended_items = recommended_items.dropna()

#     return recommended_items

