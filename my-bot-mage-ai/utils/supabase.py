from supabase import create_client, Client
import os

SUPABASE_URL: str = "https://cfivdlyedzbcvjsztebc.supabase.co"
SUPABASE_SECRET_KEY: str = os.environ.get('SUPABASE_SECRET_KEY')

def get_client():
    supabase_client: Client = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)
    return supabase_client