from supabase import create_client, Client
import os
from postgrest.exceptions import APIError
from tenacity import retry, stop_after_attempt, wait_random, retry_if_exception_type


SUPABASE_URL: str = "https://cfivdlyedzbcvjsztebc.supabase.co"
SUPABASE_SECRET_KEY: str = os.environ.get('SUPABASE_SECRET_KEY')

def get_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)


@retry(
    retry=retry_if_exception_type(APIError),
    stop=stop_after_attempt(3),
    wait=wait_random(min=5, max=20)
)
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