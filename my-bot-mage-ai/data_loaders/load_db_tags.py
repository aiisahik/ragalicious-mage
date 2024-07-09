if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from utils.supabase import get_client
import pandas as pd


@data_loader
def load_data(df_recipes, *args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    tags_to_fetch = []
    for index, row in df_recipes.iterrows():
        recipe_tags = row['features']['tags']
        if recipe_tags.any():
            tags_to_fetch += list(recipe_tags)
    # Specify your data loading logic here
    supabase_client = get_client()
    response = (
        supabase_client
        .table("tags")
        .select("tag, type")
        .in_("tag", tags_to_fetch)
        .order("tag")
        .execute()
    )
    return pd.DataFrame(response.data)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
