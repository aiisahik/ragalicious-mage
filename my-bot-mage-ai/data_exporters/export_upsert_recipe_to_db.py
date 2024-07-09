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
        records_to_upsert = df.to_dict(orient='records')
        
        response = (
            supabase_client.table("recipes")
            .upsert(
                records_to_upsert,
                on_conflict="url",
            )
            .execute()
        )
        logger.info("Upserted {len(records_to_upsert)} to recipe table")
        return len(records_to_upsert)
        
    return 0
    # Specify your data exporting logic here


