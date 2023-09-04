from collections import Counter
from numpy import argsort

def find_record(no, final_collection):
    return [ind for ind, r in enumerate(final_collection) if r[0] == no][0]

### original_collection = df.text.values
def transform_collection(original_collection, tok_to_int=None):
    q = 3
    tok_freq = Counter()
    temp_collection = []
    temp_collection_2 = []
    words_collection = []

    tokens_per_element = 0
    element_per_record = 0

    for nor, record in enumerate(original_collection):
        temp_record = []
        temp_record_2 = []
        word_record = []
        for word in record:
            word2 = word + (q - 1)*"$"
            word2 = word2[:(len(word2) - len(word2) % q)]
            tokens = [word2[i:i+q] for i in range(len(word2) - q +1)]
            
            tc = Counter(tokens)
            tokens = [f'{tok}@{v}' for tok, val in tc.items() for v in range(val)]
            
            tokens_2 = [word2[i:i+q] for i in range(0, len(word2) - q +1, q)]
            tc = Counter(tokens_2)
            tokens_2 = [f'{tok}@{v}' for tok, val in tc.items() for v in range(val)]
            
            tok_freq.update(tokens)
            temp_record.append(tokens)
            temp_record_2.append(tokens_2)
            word_record.append(word2)
            tokens_per_element += len(tokens)
            
            
        inds = argsort([len(e) for e in temp_record])
        temp_record = [temp_record[i] for i in inds]
        temp_record_2 = [temp_record_2[i] for i in inds]
        word_record = [word_record[i] for i in inds]
            
        element_per_record += len(record)
        temp_collection.append(temp_record)
        temp_collection_2.append(temp_record_2)
        words_collection.append(word_record)
        
    if tok_to_int is None:
        sorted_toks = sorted(tok_freq.items(), key=lambda x: x[1])
        tok_to_int = {tok[0]: no for no, tok in enumerate(sorted_toks)}
    else:
        neg = 0
        for tok in tok_freq:
            if tok not in tok_to_int:
                neg += 1
                tok_to_int[tok] = -neg
    
    final_collection = [(rid, [sorted([tok_to_int[tok] for tok in word]) for word in record])
                        for rid, record in enumerate(temp_collection)]
    final_collection_2 = [(rid, [sorted([tok_to_int[tok] for tok in word]) for word in record])
                        for rid, record in enumerate(temp_collection_2)]    
    

    tokens_per_element /= element_per_record;
    element_per_record /= len(final_collection)

    print("Finished reading file. Lines read: {}. Lines skipped due to errors: {}. Num of sets: {}. Elements per set: {}. Tokens per Element: {}".format(0, 0, len(final_collection), element_per_record, tokens_per_element))
    
    final_collection = sorted(final_collection, key=lambda x: len(x[1]))
    final_collection_2 = sorted(final_collection_2, key=lambda x: len(x[1]))
    final_word_collection = sorted(words_collection, key=lambda x: len(x))
    
    collection = {'collection': final_collection, 'dictionary': tok_to_int,
                  'qcollection': final_collection_2, 'words': final_word_collection}
    return collection


def build_stats_for_record(R, QR, WR, q=3):
    tokenIDs = {}
    for nor, r in enumerate(R):
        for tok in r:
            util =  1 / len(WR[nor])
            if tok not in tokenIDs:
                tokenIDs[tok] = {}
                tokenIDs[tok]['elements_2'] = []
                tokenIDs[tok]['utility_2'] = 0
                tokenIDs[tok]['utilities_2'] = []
                tokenIDs[tok]['utility'] = 0
                tokenIDs[tok]['utilities'] = []
                tokenIDs[tok]['elements'] = []
            tokenIDs[tok]['elements_2'].append(nor)
            tokenIDs[tok]['utility_2'] += util
            tokenIDs[tok]['utilities_2'].append(tokenIDs[tok]['utility_2'])
            
    for nor, r in enumerate(QR):
        for tok in r:
            util =  1 / len(WR[nor])
            if tok not in tokenIDs:
                tokenIDs[tok] = {}
                tokenIDs[tok]['elements'] = []
                tokenIDs[tok]['utility'] = 0
                tokenIDs[tok]['utilities'] = []
            tokenIDs[tok]['elements'].append(nor)
            tokenIDs[tok]['utility'] += util
            tokenIDs[tok]['utilities'].append(tokenIDs[tok]['utility'])

    UB = len(R)
    UB2 = len(R) * (q - 1) / q
    for r in WR:
        UB2 += (len(r)-q+1) / len(r) # (|r| - q) qgrams that all have

    for tok, tok_info in sorted(tokenIDs.items(), key=lambda x: x[0]):
        UB -= tok_info['utility']
        tok_info['rest'] = UB
        UB2 -= tok_info['utility_2']
        tok_info['rest_2'] = UB2
    
    return tokenIDs
    
def build_index(collection):
    lengths_list = [[] for _ in range(len(collection['dictionary']))]
    idx = []
    for noR, (idR, R) in enumerate(collection['collection']):
        tokenIDs = build_stats_for_record(collection['collection'][noR][1],
                                          collection['qcollection'][noR][1],
                                          collection['words'][noR])
        for tok in tokenIDs.keys():
            lengths_list[tok].append(noR)

        idx.append(tokenIDs) 
    return idx, lengths_list
