if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from utils.supabase import upsert_recipes
from utils.parse_html import get_snippet
from utils.choices import ParseTypes
from bs4 import BeautifulSoup
import pandas as pd

MAX_CACHE_SIZE = 5

@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    logger = kwargs.get('logger')
    TOTAL_NUM_RECIPES_TO_PARSE = kwargs.get('TOTAL_NUM_RECIPES_TO_PARSE')

    parsed_data = []
    cached_data = []
    total_upserted = 0
    total_processed = 0
    total_to_process = len(df)
    for index, row in df.iterrows():
        logger.info(f"{total_processed+1}/{total_to_process} - {row['url']}")
        soup = BeautifulSoup(row['html'], 'html.parser')
        try: 
            parsed_row_data = {
                'url': row['url'],
                'md_description': get_snippet(soup, ParseTypes.Description, markdown=True),
                'md_ingredients': get_snippet(soup, ParseTypes.Ingredients, markdown=True),
                'md_nutrition': get_snippet(soup, ParseTypes.Nutrition, markdown=True),
                'md_preparation': get_snippet(soup, ParseTypes.Preparation, markdown=True),
                'rating': get_snippet(soup, ParseTypes.Rating, markdown=False),
                'num_ratings': get_snippet(soup, ParseTypes.NumRatings, markdown=False),
                'time': get_snippet(soup, ParseTypes.TotalTime, markdown=True),
                'features': {
                    'tags': get_snippet(soup, ParseTypes.Tags, markdown=True),
                },
                'status': 'parse_success'
            }
        except: 
            parsed_row_data = {
                'url': row['url'],
                'status': 'parse_failed'
            }
        
        parsed_data.append(parsed_row_data)
        cached_data.append(parsed_row_data)
        total_processed += 1
        if len(cached_data) >= MAX_CACHE_SIZE: 
            num_upserted = upsert_recipes(cached_data, logger)
            total_upserted += num_upserted
            cached_data = []
    if len(cached_data) > 0: 
        num_upserted = upsert_recipes(cached_data, logger)
    NEW_TOTAL_NUM_RECIPES_TO_PARSE = TOTAL_NUM_RECIPES_TO_PARSE - total_upserted
    if NEW_TOTAL_NUM_RECIPES_TO_PARSE > 0:
        return 'parse_features', {
            "TOTAL_NUM_RECIPES_TO_PARSE": NEW_TOTAL_NUM_RECIPES_TO_PARSE
        }, f"parse_features {NEW_TOTAL_NUM_RECIPES_TO_PARSE}"
    return None, None, None


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
