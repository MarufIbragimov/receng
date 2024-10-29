from pathlib import Path
import numpy as np
import pandas as pd
import duckdb
import etl
from sklearn.metrics.pairwise import cosine_similarity
import streamlit as st
from sklearn.feature_extraction.text import TfidfVectorizer

def get_descriptions(items):
    df = pd.read_parquet(
        Path('data/items_descriptions.parquet'),
        columns = ['item_id', 'full_description'],
        filters = [('item_id', 'in', items)]
    )

    return df


def vectorize(items):
    descriptions_df = get_descriptions(items)

    vectorizer = TfidfVectorizer(min_df = .1, max_df = .7)

    vectorized_data = vectorizer.fit_transform(
        descriptions_df['full_description']
    )

    tfidf_df = pd.DataFrame(
        vectorized_data.toarray(),
        columns = vectorizer.get_feature_names_out(),
        index = descriptions_df['item_id']
    )

    return tfidf_df



def recommend(user: int, filtered_df: pd.DataFrame):
    vector = vectorize(filtered_df['item_id'].values.tolist())

    user_items = filtered_df.query(f"user_id == {user}")['item_id'].values.tolist()
    user_mean_vector = vector.reindex(user_items).mean()

    non_user_vector = vector.drop(user_items, axis=0)

    similar_items = cosine_similarity(
        user_mean_vector.values.reshape(1,-1),
        non_user_vector
    ) 

    similar_items_df = (
        pd.DataFrame(
            similar_items.T,
            index = non_user_vector.index,
            columns = ['similarity_score']
        )
        .sort_values(by=['similarity_score'], ascending = False)
        .head(20)
    )
     

    return similar_items_df