if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import re
import pandas as pd
from langchain_community.document_transformers import MarkdownifyTransformer
from langchain_community.document_transformers import BeautifulSoupTransformer
from utils.scrape import spider_scrape

md = MarkdownifyTransformer()
bs4_transformer = BeautifulSoupTransformer()


def find_image(markdown):
    # Regular expression to find all https:// links in the string
    pattern = r'\((https://static[^\s]+)\)'
    # Find all matches using the regex pattern
    images = re.findall(pattern, markdown)
    return images

def clean_html(html:str) -> str:
    cleaned_html = bs4_transformer.remove_unwanted_classnames(
        html, 
        unwanted_classnames=[
            re.compile("^header_staticHeaderContainer__"),
            re.compile("^carousel_carousel__"),
            re.compile("^notessection_notesSection__"),
            re.compile("^footer_footer__"),
            re.compile("^recipeintro_tools-block__"),
            ## ratings section
            re.compile("^ratingssection_userRatingsHeader__"),
            re.compile("^userratingstars_userRatingStarsContainer__"),
            re.compile("ratingssection_subsectionLabel__"),
            ## notes
            re.compile("^notessection_notesPrintLayout__"),
            re.compile("ingredients_edamamLink__"),
            re.compile("^topnote_relatedArticle__"),
            re.compile("recipeheaderimage_credit__"),
            re.compile("^adunit_ad-"),
        ]
    )
    cleaned_html = (
        cleaned_html
        .replace("</dd>", "</dd><p></p><br/>")
        .replace("</dt>", ":</dt>")
    )
    return cleaned_html


@data_loader
def load_data(df, *args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your data loading logic here
    scraped_recipes = []
    for index, row in df.iterrows(): 
        
        logger = kwargs.get('logger')
        logger.info(f"scraping {row['url']}")
        recipe_docs = spider_scrape(
            row['url'], 
            params={"return_format": "raw", "query_selector": "main"}, 
        )
        if len(recipe_docs) > 0:
            recipe_doc = recipe_docs[0]
            
            recipe_doc.page_content = clean_html(recipe_doc.page_content)
            markdown_receipe_docs = md.transform_documents([recipe_doc])

            images = find_image(markdown_receipe_docs[0].page_content)
            metadata = recipe_doc.metadata

            scraped_recipes.append({
                'url': row['url'],
                'html': recipe_doc.page_content,
                'metadata': {**metadata, "thumbnail": images[0] if len(images) else None },
                'markdown': markdown_receipe_docs[0].page_content,
            })
        else: 
            logger.warn(f"URL not found: {row['url']}")
    

    return pd.DataFrame(scraped_recipes)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
