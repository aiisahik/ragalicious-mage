if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from mage_ai.orchestration.triggers.api import trigger_pipeline

@custom
def transform_custom(combined_inputs, *args, **kwargs):

    pipeline_name, pipeline_variables, schedule_name = combined_inputs

    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """

    logger = kwargs.get('logger')
    if pipeline_name:
        logger.info(f"Triggering {pipeline_name} - {schedule_name}")
        # Specify your custom logic here
        trigger_pipeline(
            pipeline_name,
            variables=pipeline_variables,
            check_status=False,
            error_on_failure=False,
            poll_interval=60,
            schedule_name=schedule_name,  # Enter a unique name to create a new trigger each time
            verbose=True,
        )
    else: 
        logger.info(f"Ending")

    return 1


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
