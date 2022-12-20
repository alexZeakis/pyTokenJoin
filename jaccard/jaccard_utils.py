from collections import Counter

### original_collection = df.text.values
def transform_collection(original_collection, tok_to_int=None):
    q = 3
    tok_freq = Counter()
    temp_collection = []

    tokens_per_element = 0
    element_per_record = 0

    for nor, record in enumerate(original_collection):
        temp_record = []
        for word in record:
            
            word2 = word + (q - 1)*"$"
            word2 = word2[:(len(word2) - len(word2) % q)]
            #word2 = word + (q - (len(word) % q))*"$"
            tokens = [word2[i:i+q] for i in range(len(word2) - q +1)]
            
            tc = Counter(tokens)
            tokens = [f'{tok}@{v}' for tok, val in tc.items() for v in range(val)]
            
            tok_freq.update(tokens)
            temp_record.append(tokens)
            tokens_per_element += len(tokens)

        element_per_record += len(record)
        temp_collection.append(temp_record)

    if tok_to_int is None:
        sorted_toks = sorted(tok_freq.items(), key=lambda x: x[1])
        tok_to_int = {tok[0]: no for no, tok in enumerate(sorted_toks)}
    else:
        neg = 0
        for tok in tok_freq:
            if tok not in tok_to_int:
                neg += 1
                tok_to_int[tok] = -neg        
    
    final_collection = [(rid, sorted([sorted([tok_to_int[tok] for tok in word]) for word in record], key=lambda x: len(x)))
                        for rid, record in enumerate(temp_collection)]

    tokens_per_element /= element_per_record;
    element_per_record /= len(final_collection)

    print("Finished reading file. Lines read: {}. Lines skipped due to errors: {}. Num of sets: {}. Elements per set: {}. Tokens per Element: {}".format(0, 0, len(final_collection), element_per_record, tokens_per_element))
    
    final_collection = sorted(final_collection, key=lambda x: len(x[1]))
    
    collection = {'collection': final_collection, 'dictionary': tok_to_int}
    return collection
    
    #return final_collection, tok_to_int
    #final_collection


def build_stats_for_record(R):
    tokenIDs = {}
    for nor, r in enumerate(R):
        for tok in r:
            util = 1/len(r)
            if tok not in tokenIDs:
                tokenIDs[tok] = {}
                tokenIDs[tok]['elements'] = []
                tokenIDs[tok]['utility'] = 0
                tokenIDs[tok]['utilities'] = []
            tokenIDs[tok]['elements'].append(nor)
            tokenIDs[tok]['utility'] += util
            tokenIDs[tok]['utilities'].append(tokenIDs[tok]['utility'])

    UB = len(R)
    for tok, tok_info in sorted(tokenIDs.items(), key=lambda x: x[0]):
        UB -= tok_info['utility']
        tok_info['rest'] = UB
    
    return tokenIDs
    
def build_index(collection):
    lengths_list = [[] for _ in range(len(collection['dictionary']))]
    idx = []
    for noR, (idR, R) in enumerate(collection['collection']):
        tokenIDs = build_stats_for_record(R)
        for tok in tokenIDs.keys():
            lengths_list[tok].append(noR)

        idx.append(tokenIDs) 
    return idx, lengths_list