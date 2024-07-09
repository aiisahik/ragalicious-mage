if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import re
import pandas as pd
from utils.scrape import get_hrequests_fn
from utils.supabase import get_client, upsert_recipes
from markdownify import markdownify as md


def find_image(markdown):
    # Regular expression to find all https:// links in the string
    pattern = r'\((https://static[^\s]+)\)'
    # Find all matches using the regex pattern
    images = re.findall(pattern, markdown)
    return images

clean_queries = [
    '[class^="header_staticHeaderContainer__"]',
    '[class^="carousel_carousel__"]',
    '[class^="notessection_notesSection__"]',
    '[class^="footer_footer__"]',
    '[class^="recipeintro_tools-block__"]',
    ## ratings section
    '[class^="ratingssection_userRatingsHeader__"]',
    '[class^="userratingstars_userRatingStarsContainer__"]',
    '[class*="ratingssection_subsectionLabel__"]',
    ## notes
    '[class^="notessection_notesPrintLayout__"]',
    '[class*="ingredients_edamamLink__"]',
    '[class^="topnote_relatedArticle__"]',
    '[class*="recipeheaderimage_credit__"]',
    '[class^="adunit_ad-"]',
]

def clean_html(node):
    for query in clean_queries:
        node_to_remove = node.find(query)
        if node_to_remove: 
            node_to_remove.element.remove()
    return node


MAX_CACHE_SIZE = 20

@data_loader
def load_data(df, *args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your data loading logic here
    logger = kwargs.get('logger')
    hrequests_scrape_fn, hrequests_session = get_hrequests_fn(logger)
    
    scraped_recipes = []
    upsert_cache = []
    num_total_to_process = len(df)
    for index, row in df.iterrows(): 
        logger.info(f"scraping {row['url']}")
        html_obj = hrequests_scrape_fn(
            row['url'], 
            session=hrequests_session
        )
        scraped_row = None
        if html_obj:
            title = html_obj.find("title").text
            main_obj = html_obj.find("main")
            if main_obj:
                cleaned_main_obj = clean_html(main_obj)
                markdown = md(cleaned_main_obj.html, strip=['a']) 

                images = find_image(markdown)
                # metadata = recipe_doc.metadata
                scraped_row = {
                    'url': row['url'],
                    'html': main_obj.html,
                    'metadata': {
                        "thumbnail": images[0] if len(images) else None,
                        "title": title,
                    },
                    'markdown': markdown,
                    'status': 'scrape_success'
                }
        if not scraped_row:
            scraped_row = {
                'url': row['url'],
                'status': 'scrape_failed'
            }
            logger.warn(f"Scrape failed: {row['url']}")
        
        scraped_recipes.append(scraped_row)
        upsert_cache.append(scraped_row)

        if len(upsert_cache) >= MAX_CACHE_SIZE:
            logger.info(f'{len(scraped_recipes)} / {num_total_to_process} Upserting to DB')
            upsert_recipes(upsert_cache, logger)
            upsert_cache = [] 
        else: 
            logger.info(f'{len(scraped_recipes)} / {num_total_to_process} Scraped to Cache')

    if len(upsert_cache) > 0:
        logger.info(f'{len(scraped_recipes)} / {num_total_to_process} Upserting to DB')
        upsert_recipes(upsert_cache, logger)

    hrequests_session.close()
    return len(scraped_recipes)

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
