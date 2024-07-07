if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from utils.supabase import upsert_recipes
from utils.parse_html import get_snippet
from utils.choices import ParseTypes
from bs4 import BeautifulSoup
import pandas as pd

MAX_CACHE_SIZE = 10

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

    parsed_data = []
    cached_data = []
    total_upserted = 0
    for index, row in df.iterrows():
        print('parsing', row['url'])
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
                'status': 'parsed'
            }
            print(parsed_row_data)
        except: 
            parsed_row_data = {
                'url': row['url'],
                'status': 'parse_failed'
            }
        parsed_data.append(parsed_row_data)
        cached_data.append(parsed_row_data)

        if len(cached_data) > MAX_CACHE_SIZE: 
            num_upserted = upsert_recipes(cached_data)
            total_upserted += 0
            cached_data = []

    return pd.DataFrame(parsed_data)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
