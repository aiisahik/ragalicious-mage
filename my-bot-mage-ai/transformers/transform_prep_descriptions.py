if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from utils.choices import TagTypes
from collections import defaultdict
import pandas as pd
from utils.strings import to_human_list


def get_recipe_tag_type_collection(row, tags_dict):
    recipe_tag_type_collection = defaultdict(list)
    recipe_tags = None
    if isinstance(row['features'], dict) and 'tags' in row['features']:
       recipe_tags = row['features']['tags']
    if recipe_tags and isinstance(recipe_tags, list):
        for recipe_tag in recipe_tags: 
            recipe_tag_type = tags_dict.get(recipe_tag)
            if recipe_tag_type:
                recipe_tag_type_collection[recipe_tag_type].append(recipe_tag)
    return recipe_tag_type_collection

def get_description_chunk(row, recipe_tag_type_collection) -> str:
    description = ""
    title = row['metadata'].get('title')
    if title:
        description += title
    
    if recipe_tag_type_collection:
        
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

        tags_equipment = recipe_tag_type_collection[TagTypes.Equipment.value]
        if tags_equipment:
            description += f"""\n\nThis recipe makes use of this equipment: {to_human_list(tags_equipment)}."""
    
    description += f"""\n\n{row['md_description']}"""
    return description

def get_ingredients_chunk(row, recipe_tag_type_collection) -> str:
    output = ""
    
    if recipe_tag_type_collection:
        
        tags_ingredients = recipe_tag_type_collection[TagTypes.Ingredient.value]
        if tags_ingredients:
            output += f"""This recipe uses the following primary ingredients: {to_human_list(tags_ingredients)}.\n\n"""
    
    output += f"""{row['md_ingredients']}"""
    if output: 
        title = row['metadata'].get('title')
        if title:
            output = f"""{title}\n\n{output}"""
    return output

def get_nutrition_chunk(row, recipe_tag_type_collection) -> str:
    output = ""
    
    if recipe_tag_type_collection:
        
        tags_ingredients = recipe_tag_type_collection[TagTypes.Ingredient.value]
        if tags_ingredients:
            output += f"""\n\nThis recipe uses the following primary ingredients: {to_human_list(tags_ingredients)}."""
    
    output += f"""\n\n{row['md_nutrition']}"""
    return output

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
        
        recipe_tag_type_collection = get_recipe_tag_type_collection(row, tags_dict)

        output.append({
            'url': row['url'],
            "description": get_description_chunk(row, recipe_tag_type_collection),
            "ingredients": get_ingredients_chunk(row, recipe_tag_type_collection),
            "nutrition": get_nutrition_chunk(row, recipe_tag_type_collection),
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
