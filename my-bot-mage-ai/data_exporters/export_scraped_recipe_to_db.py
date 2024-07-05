if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from mage_ai.orchestration.triggers.api import trigger_pipeline
from utils.supabase import get_client

@data_exporter
def export_data(df, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    supabase_client = get_client()
    logger = kwargs.get('logger')

    if len(df) > 0:
        scraped_records = df.to_dict(orient='records')
    
        response = (
            supabase_client.table("recipes")
            .upsert(
                scraped_records,
                on_conflict="url",
            )
            .execute()
        )
        # print(response.data)
        num_upserted = len(response.data)

        logger.info(f"Upserted {num_upserted} scraped recipies")

        TOTAL_NUM_RECIPIES_TO_SCRAPE = kwargs['TOTAL_NUM_RECIPIES_TO_SCRAPE']
        NUM_RECIPIES_TO_SCRAPE_PER_RUN = kwargs['NUM_RECIPIES_TO_SCRAPE_PER_RUN']

        NEW_TOTAL_NUM_RECIPIES_TO_SCRAPE = TOTAL_NUM_RECIPIES_TO_SCRAPE - num_upserted
        if NEW_TOTAL_NUM_RECIPIES_TO_SCRAPE<= 0:
            logger.info(f"Target TOTAL_NUM_RECIPIES_TO_SCRAPE {TOTAL_NUM_RECIPIES_TO_SCRAPE} reached")
            return None
        
        logger.info(f"Re-running with TOTAL_NUM_RECIPIES_TO_SCRAPE: {TOTAL_NUM_RECIPIES_TO_SCRAPE}")

        schedule_name = kwargs.get('pipeline_schedule_name')
        print(schedule_name)
        trigger_pipeline(
            'recipes',
            variables={
                'TOTAL_NUM_RECIPIES_TO_SCRAPE': NEW_TOTAL_NUM_RECIPIES_TO_SCRAPE,
                'NUM_RECIPIES_TO_SCRAPE_PER_RUN': NUM_RECIPIES_TO_SCRAPE_PER_RUN,
            },
            check_status=False,
            error_on_failure=False,
            poll_interval=60,
            poll_timeout=None,
            schedule_name=f"recipes: {NEW_TOTAL_NUM_RECIPIES_TO_SCRAPE}",  # Enter a unique name to create a new trigger each time
            verbose=True,
        )


    else: 
        logger.warn(f"No records to upsert")

