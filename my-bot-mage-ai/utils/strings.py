def to_human_list(l:list) -> str:
    output = ""
    num_items = len(l)
    if num_items == 1:
        return l[0]
    for index, item in enumerate(l): 
        if index == (num_items - 1):
            output += f"and {item}"
        elif num_items == 2:
            output += f"{item} " 
        else:
            output += f"{item}, "
    return output