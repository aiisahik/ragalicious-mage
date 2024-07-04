if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

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
        print(num_upserted)

        logger.info(f"Upserted {num_upserted} scraped recipies")
    else: 
        logger.warn(f"No records to upsert")
    # print(len(df))
    # print(type(df))
    
    # Specify your data exporting logic here


