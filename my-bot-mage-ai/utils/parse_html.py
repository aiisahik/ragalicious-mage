from bs4 import BeautifulSoup
from bs4.element import ResultSet 
import re
from markdownify import markdownify as md
import copy
from .choices import ParseTypes


def get_nutrition(soup):
    tag = soup.find(class_=re.compile("^ingredients_buttons__"))
    if tag:
        tag = tag.find(class_=re.compile("^popover_popover-body--message__"))
    if tag:
        tag.find(class_=re.compile("^pantry--ui-xs")).decompose()
    return str(tag) if tag else None

def get_ingredients(soup):
    tag_ingredients = soup.find(class_=re.compile("^ingredients_ingredients__"))
    for el in tag_ingredients.find_all("span"): 
        el.append(" ")
    tag_ingredients.find(class_=re.compile("^ingredients_buttons__")).decompose()
  
    return str(tag_ingredients)


def get_tags(soup):
    tag = soup.find(class_=re.compile("^creditstags_tags__"))
    res = tag.find_all('a')
    return res

def get_total_time(soup) -> int:
    time_text = str(soup.find(class_=re.compile("^stats_cookingTimeTable__")).find(class_='pantry--ui'))
    if time_text:
        # Define the regex patterns for hours and minutes
        hour_pattern = re.compile(r'(\d+)\s*hour')
        minute_pattern = re.compile(r'(\d+)\s*minute')
        
        # Find matches in the text
        hour_match = hour_pattern.search(time_text)
        minute_match = minute_pattern.search(time_text)
        
        # Extract the hour and minute values, defaulting to 0 if not found
        hours = int(hour_match.group(1)) if hour_match else 0
        minutes = int(minute_match.group(1)) if minute_match else 0
        
        return hours * 60 + minutes
    
    return None

def get_rating(soup):
    tag = soup.find(class_=re.compile("^ratingssection_ratingsInfoText__")).find(class_="pantry--ui-lg-strong")
    if tag:
        rating_str = str(tag).replace(' out of 5', '')
        rating_num_str = md(rating_str)
        if rating_num_str:
            return int(rating_num_str)
    return None

def get_num_ratings(soup):
    tag = soup.find(class_=re.compile("^ratingssection_ratingsCount__"))
    if tag:
            rating_str = str(tag).replace(" user ratings", "").replace(",", "")
            rating_num_str = md(rating_str)
            if rating_num_str:
                return int(rating_num_str)
    return None

BS4_PARSER_MAP = {
    ParseTypes.Description.value: lambda soup: str(soup.find(class_=re.compile("^topnote_topnote__"))),
    ParseTypes.Preparation.value: lambda soup: str(soup.find(class_=re.compile("^recipebody_prep-block__"))),
    ParseTypes.Rating.value: get_rating,
    ParseTypes.NumRatings.value: get_num_ratings,
    ParseTypes.TotalTime.value: get_total_time,
    ParseTypes.Ingredients.value: get_ingredients,
    ParseTypes.Nutrition.value: get_nutrition,
    ParseTypes.Tags.value: get_tags,
}


def get_snippet(soup, parse_type: ParseTypes, markdown: bool=True) -> str:
    copied_soup = copy.deepcopy(soup) ## need to make sure the original soup is not mutated
    parser_fn = BS4_PARSER_MAP[parse_type.value]
    if not parser_fn: 
        raise ValueError(f'unable to find {parse_type}')
    tag = parser_fn(copied_soup)
    if tag and markdown:
        if isinstance(tag, ResultSet):
            return [md(str(el), strip=['a']) for el in tag]  
        return md(str(tag), strip=['a'])
    return tag