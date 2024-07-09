if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from utils.choices import TagTypes
from collections import defaultdict
import pandas as pd
from utils.strings import to_human_list

@transformer
def transform(df_recipes, df_tags, *args, **kwargs):
    """
    Takes the raw data from the recipes table and generates 
    the RECIPE DESCRIPTIONS 
    that can be directly injected into a vectordb
    """
    # Specify your transformation logic here
    
    tags_dict = dict(zip(df_tags.iloc[:, 0], df_tags.iloc[:, 1]))
    output = []
    for index, row in df_recipes.iterrows():
        
        description = ""
        title = row['metadata'].get('title')
        if title:
            description += title
        
        if isinstance(row['features'], dict) and 'tags' in row['features']:

            recipe_tags = row['features']['tags']
            recipe_tag_type_collection = defaultdict(list)
            if recipe_tags:
                for recipe_tag in recipe_tags: 
                    recipe_tag_type = tags_dict.get(recipe_tag)
                    if recipe_tag_type:
                        recipe_tag_type_collection[recipe_tag_type].append(recipe_tag)
            
            tags_meal = recipe_tag_type_collection[TagTypes.Meal.value]
            if tags_meal:
                description += f"""\n\nThis recipe creates this type of meal or dish: {to_human_list(tags_meal)}."""

            tags_occasion = recipe_tag_type_collection[TagTypes.Occasion.value]
            if tags_occasion:
                description += f"""\n\nThis recipe is suitable for these situations and occasions: {to_human_list(tags_occasion)}."""
            
            tags_cuisine = recipe_tag_type_collection[TagTypes.Cuisine.value]
            if tags_cuisine:
                description += f"""\n\nThis recipe belongs to these cuisines: {to_human_list(tags_cuisine)}."""
            
            tags_diet = recipe_tag_type_collection[TagTypes.Diet.value]
            if tags_diet:
                description += f"""\n\nThis recipe conforms to these diets: {to_human_list(tags_diet)}."""
        
        description += f"""\n\n{row['md_description']}"""

        output.append({
            'url': row['url'],
            "page_content": description,
            "metadata": {
                'id': row['id'],
                'url': row['url'],
                'num_ratings': row['num_ratings'],
                'rating': row['rating'],
            }
        })
    
    return pd.DataFrame(output)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
