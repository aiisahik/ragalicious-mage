import numpy

def normalize_metadata(input_metadata:dict) -> dict:
    output_metadata = {}
    for k, v in input_metadata.items():
        if isinstance(v, numpy.ndarray): 
            if len(v) > 0:
                output_metadata[k] = v.tolist()
        elif isinstance(v, list): 
            if v and len(v) > 0:
                output_metadata[k] = v
        else: 
            output_metadata[k] = v
    
    return output_metadata