if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def transform_custom(num_recipes_populated, *args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your custom logic here
    TOTAL_NUM_RECIPES = kwargs.get('TOTAL_NUM_RECIPES')
    
    NEW_TOTAL_NUM_RECIPES = max(0, TOTAL_NUM_RECIPES - num_recipes_populated)

    if NEW_TOTAL_NUM_RECIPES > 0:
        return 'populate_vector_db', {
                "TOTAL_NUM_RECIPES": NEW_TOTAL_NUM_RECIPES
            }, f"populate_vector_db {NEW_TOTAL_NUM_RECIPES}"
    return None, {}, 'done'


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
