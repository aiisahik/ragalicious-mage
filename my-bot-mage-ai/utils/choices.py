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