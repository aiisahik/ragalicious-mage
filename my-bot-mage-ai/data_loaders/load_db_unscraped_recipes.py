if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from utils.supabase import get_client
import pandas as pd

import os 

def chunk_list(lst:list, n:int) -> list:
    # Using list comprehension to split list into chunks of size n
    return [lst[i:i + n] for i in range(0, len(lst), n)]

import json

# MAX_NUM_URLS_PER_BLOCK = 10

@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your data loading logic here

    NUM_RECIPIES_TO_SCRAPE = kwargs['NUM_RECIPIES_TO_SCRAPE']
    logger = kwargs.get('logger')

    supabase_client = get_client()
    response = (
        supabase_client
        .table("recipes")
        .select("url")
        .is_("html", "null")
        .limit(NUM_RECIPIES_TO_SCRAPE)
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
