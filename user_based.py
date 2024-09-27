from pathlib import Path
# import pandas as pd
import duckdb
import etl
from sklearn.metrics.pairwise import cosine_similarity


def ratings_matrice(selected_date):
    ratings_df = (
        etl.get_reviews(selected_date)
        .pivot_table(
            index = 'user_id',
            columns = 'item_id',
            values = 'rating'
        )
    )

    avg_ratings = ratings_df.mean(axis = 1)

    centered_ratings = ratings_df.sub(avg_ratings, axis = 0).fillna(0)

    return centered_ratings.head(20)
