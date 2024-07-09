from enum import Enum


class ParseTypes(Enum):
    Description = 'description'
    Preparation = 'preparation'
    Rating = 'rating'
    NumRatings = 'num_ratings'
    TotalTime = 'total_time'
    Ingredients = 'ingredients'
    Nutrition = 'nutrition'
    Tags = 'tags'



class TagTypes(Enum):
    Occasion = "Occasion"
    Cuisine = "Cuisine"
    Ingredient = "Ingredient"
    Difficulty = "Difficulty"
    Equipment = "Equipment"
    Meal = "Meal"