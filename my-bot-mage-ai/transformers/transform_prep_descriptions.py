if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from utils.choices import TagTypes
from collections import defaultdict

@transformer
def transform(df_recipes, df_tags, *args, **kwargs):
    """
    Takes the raw data from the recipes table 
    and generates data that can be directly injected 
    into a vectordb
    """
    # Specify your transformation logic here
    
    tags_dict = dict(zip(df_tags.iloc[:, 0], df_tags.iloc[:, 1]))
    print(tags_dict)
    output = []
    for index, row in df_recipes.iterrows():
        recipe_tags = row['features']['tags']
        recipe_tag_type_collection = defaultdict(list)
        if recipe_tags.any():
            for recipe_tag in recipe_tags: 
                recipe_tag_type = tags_dict.get(recipe_tag)
                if recipe_tag_type:
                    recipe_tag_type_collection[recipe_tag_type].append(recipe_tag)

        print(
            row['url'],
            recipe_tag_type_collection
        )

        output.append({
            "page_content": row['md_description'],
            "metadata": {
                'url': row['url'],
                'num_ratings': row['num_ratings'],
                'rating': row['rating'],
            }
        })
    
    return 1


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
