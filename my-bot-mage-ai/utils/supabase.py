from supabase import create_client, Client
import os

SUPABASE_URL: str = "https://cfivdlyedzbcvjsztebc.supabase.co"
SUPABASE_SECRET_KEY: str = os.environ.get('SUPABASE_SECRET_KEY')

def get_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)

def upsert_recipes(data: list, logger) -> int:
    supabase_client = get_client()
    response = (
        supabase_client.table("recipes")
        .upsert(
            data,
            on_conflict="url",
        )
        .execute()
    )
    num_upserted = len(response.data)
    logger.info(f"Upserted {num_upserted} scraped recipies")
    return len(response.data)