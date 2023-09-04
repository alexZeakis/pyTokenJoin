def binary_search(arr, x):
    low = 0
    high = len(arr) - 1
    mid = 0

    while low <= high:
        mid = (high + low) // 2

        # If x is greater, ignore left half
        if arr[mid] < x:
            low = mid + 1
        # If x is smaller, ignore right half
        elif arr[mid] > x:
            high = mid - 1
        # means x is present at mid
        else:
            return mid
    # If we reach here, then the element was not present
    #return -1
    return mid  

def binary_search_dupl(arr, x, collection):
    low = 0
    high = len(arr) - 1
    mid = 0

    while low <= high:
        mid = (high + low) // 2

        # If x is greater, ignore left half
        if len(collection[arr[mid]]) < x:
            low = mid + 1
        # If x is smaller, ignore right half
        elif len(collection[arr[mid]]) > x:
            high = mid - 1
        # means x is present at mid
        else:
            return mid
    # If we reach here, then the element was not present
    #return -1
    return mid 

def post_basic(R, S, tokens, idx, pers_delta, total, pos_tok):
    for (tok, tok_info) in tokens[pos_tok:]:
        if tok not in idx[S]:
            total -= tok_info['utility']
            
        if pers_delta - total > .0000001:
            return total
    return total

def post_positional(R, S, tokens, idx, pers_delta, util_gathered, sum_stopped, pos_tok):
    for (tok, tok_info) in tokens[pos_tok:]:
        sum_stopped -= tok_info['utility']
        if tok in idx[S]:
            tok_info_S = idx[S][tok]
            util_gathered += tok_info['utility']

            if pers_delta - (util_gathered + sum_stopped) > .0000001:
                return (util_gathered + sum_stopped)
            
            if pers_delta - (util_gathered + tok_info_S['rest']) > .0000001:
                return (util_gathered + tok_info_S['rest'])
        else:
            if pers_delta - (util_gathered + sum_stopped) > .0000001:
                return (util_gathered + sum_stopped)
    return (util_gathered + sum_stopped)