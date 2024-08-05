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
    
    TOTAL_NUM_RECIPES_TO_PARSE = kwargs.get('TOTAL_NUM_RECIPES_TO_PARSE')
    # TOTAL_NUM_RECIPES_TO_PARSE = 2
    NUM_RECIPES_TO_PARSE_PER_RUN = kwargs.get('NUM_RECIPES_TO_PARSE_PER_RUN')
    RECIPE_STATUS_INPUT = kwargs.get('RECIPE_STATUS_INPUT')

    num_recipes_to_query = min(NUM_RECIPES_TO_PARSE_PER_RUN, TOTAL_NUM_RECIPES_TO_PARSE)
    logger.info(f'Fetching unscraped recipies: {num_recipes_to_query}/{TOTAL_NUM_RECIPES_TO_PARSE}')

    supabase_client = get_client()
    response = (
        supabase_client
        .table("recipes")
        .select("html, url")
        .eq("status", RECIPE_STATUS_INPUT)
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
