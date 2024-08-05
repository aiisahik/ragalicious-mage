if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from utils.supabase import get_client
import pandas as pd
import os 
import json

@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your data loading logic here
    logger = kwargs.get('logger')
    
    TOTAL_NUM_RECIPES = kwargs.get('TOTAL_NUM_RECIPES')
    # TOTAL_NUM_RECIPES = 10
    NUM_RECIPES_PER_RUN = kwargs.get('NUM_RECIPES_PER_RUN')

    num_recipes_to_query = min(NUM_RECIPES_PER_RUN, TOTAL_NUM_RECIPES)
    logger.info(f'Fetching parsed recipes: {num_recipes_to_query}/{TOTAL_NUM_RECIPES}')

    supabase_client = get_client()
    response = (
        supabase_client
        .table("recipes")
        .select("id, status, url, metadata, num_ratings, rating, time, features, md_description, md_ingredients, md_nutrition")
        .in_("status", ["vector_success", "parse_success", "vector_metadata_success"])
        .gte('num_ratings', 10)
        .gte('rating', 4)
        .gt('time', 0)
        .not_.is_("md_description", "null")
        .not_.eq("md_description", "")
        .not_.is_("md_nutrition", "null")
        .not_.eq("md_nutrition", "")
        .not_.is_("md_ingredients", "null")
        .not_.eq("md_ingredients", "")
        .order("num_ratings", desc=True)
        .limit(num_recipes_to_query)
        .execute()
    )
    logger.info(f"Retrieved {len(response.data)} receipes to scrape")
    return pd.DataFrame(response.data)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
