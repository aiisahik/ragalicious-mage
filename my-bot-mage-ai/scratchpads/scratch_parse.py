from utils.supabase import upsert_recipes
from utils.parse_html import get_snippet
from utils.choices import ParseTypes
from bs4 import BeautifulSoup
from utils.supabase import get_client



supabase_client = get_client()
response = (
    supabase_client
    .table("recipes")
    .select("html, url")
    .eq("status", "scrape_success")
    .is_("md_description", "null")
    .limit(1)
    .execute()
)

row = response.data[0]

soup = BeautifulSoup(row['html'], 'html.parser')
tags = get_snippet(soup, ParseTypes.Tags, markdown=True)
print(tags)

