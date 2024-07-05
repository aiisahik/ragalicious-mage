if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def transform_custom(num_upserted_this_run: int, *args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    logger = kwargs.get('logger')
    
    TOTAL_NUM_RECIPIES_TO_SCRAPE = kwargs['TOTAL_NUM_RECIPIES_TO_SCRAPE']
    NUM_RECIPIES_TO_SCRAPE_PER_RUN = kwargs['NUM_RECIPIES_TO_SCRAPE_PER_RUN']

    NEW_TOTAL_NUM_RECIPIES_TO_SCRAPE = TOTAL_NUM_RECIPIES_TO_SCRAPE - num_upserted_this_run
    if NEW_TOTAL_NUM_RECIPIES_TO_SCRAPE<= 0:
        logger.info(f"Target TOTAL_NUM_RECIPIES_TO_SCRAPE {TOTAL_NUM_RECIPIES_TO_SCRAPE} reached")
        return None
    
    logger.info(f"Re-running with TOTAL_NUM_RECIPIES_TO_SCRAPE: {TOTAL_NUM_RECIPIES_TO_SCRAPE}")

    trigger_pipeline(
        'recipes',
        variables={
            'TOTAL_NUM_RECIPIES_TO_SCRAPE': NEW_TOTAL_NUM_RECIPIES_TO_SCRAPE,
            'NUM_RECIPIES_TO_SCRAPE_PER_RUN': NUM_RECIPIES_TO_SCRAPE_PER_RUN,
        },
        check_status=True,
        error_on_failure=True,
        poll_interval=60,
        poll_timeout=60*20,
        schedule_name=f"recipes: {NEW_TOTAL_NUM_RECIPIES_TO_SCRAPE}",  # Enter a unique name to create a new trigger each time
        verbose=True,
    )

    return True


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
