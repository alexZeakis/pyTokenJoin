from collections import Counter
from math import floor, ceil
import pandas as pd
from time import time
from numpy import argsort
from verification import verification, neds

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
            
        # if nor == 27:
        #     print(temp_record)
        #     print(temp_record_2)
        #     print(word_record)
            
        element_per_record += len(record)
        temp_collection.append(temp_record)
        temp_collection_2.append(temp_record_2)
        words_collection.append(word_record)
        
    if tok_to_int is None:
        sorted_toks = sorted(tok_freq.items(), key=lambda x: x[1])
        tok_to_int = {tok[0]: no for no, tok in enumerate(sorted_toks)}
    #else:
    #    pass
    #    #for tok 
    #    #tok_to_int = {tok[0]: no for no, tok in enumerate(sorted_toks)}
    
    # final_collection = [(rid, sorted([sorted([tok_to_int[tok] for tok in word]) for word in record], key=lambda x: len(x)))
    #                     for rid, record in enumerate(temp_collection)]
    # final_collection_2 = [(rid, sorted([sorted([tok_to_int[tok] for tok in word]) for word in record], key=lambda x: len(x)))
    #                     for rid, record in enumerate(temp_collection_2)]
    
    final_collection = [(rid, [sorted([tok_to_int[tok] for tok in word]) for word in record])
                        for rid, record in enumerate(temp_collection)]
    final_collection_2 = [(rid, [sorted([tok_to_int[tok] for tok in word]) for word in record])
                        for rid, record in enumerate(temp_collection_2)]    
    

    tokens_per_element /= element_per_record;
    element_per_record /= len(final_collection)

    print("Finished reading file. Lines read: {}. Lines skipped due to errors: {}. Num of sets: {}. Elements per set: {}. Tokens per Element: {}".format(0, 0, len(final_collection), element_per_record, tokens_per_element))
    
    #Could possibly sort one for indexes and then all, to be sure sorting is the same
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
        
        #if noR == 0:
        #    #print(tokenIDs)
        #    for tok, tok_info in tokenIDs.items():
        #        print(tok, tok_info['rest'], tok_info['rest_2'])
        for tok in tokenIDs.keys():
            lengths_list[tok].append(noR)

        idx.append(tokenIDs) 
    return idx, lengths_list

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


def post_joint(R, S, tokens, idx, pers_delta, util_gathered, sum_stopped, pos_tok):
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
      
    total = util_gathered + sum_stopped
    
    for (tok, tok_info) in tokens:
        if tok in idx[S]:
            total -= tok_info['utility']
            
            tok_info_S = idx[S][tok]
            minLen = min(len(tok_info['utilities_2']), len(tok_info_S['utilities_2'])) - 1
            util_score = min(tok_info['utilities_2'][minLen], tok_info_S['utilities_2'][minLen])
            total += util_score

        if pers_delta - total > .0000001:
            return total
        
    return total    





def simjoin(collection1, collection2, delta, idx, lengths_list, jointFilter, posFilter):

    selfjoin = collection1 == collection2
    
    init_time = candgen_time = candref_time = candver_time = 0
    no_candgen = no_candref = no_candver = no_candres = 0
    output = []
    for R in range(len(collection1['words'])):
        
        if R % 100 == 0:
            print("Progress {:,}/{:,} \r".format(R, len(collection1['collection'])), end='')
        
        # if R != 348:
        #     continue

        t1 = time()
        ## Starting Initialization ##
        (R_id, _) = collection1['collection'][R]
        R_rec = collection1['words'][R]
        RLen = len(R_rec)
        
        if selfjoin:
            tokens = idx[R]
        else:
            tokens = build_stats_for_record(collection1['collection'][R][1],
                                            collection1['qcollection'][R][1],
                                            collection1['words'][R])
        
        tokens = sorted(tokens.items(), key=lambda x: x[0])
        sum_stopped = RLen
        
        RLen_max = floor(RLen / delta)
        
        if selfjoin:
            theta = 2 * delta / (1 + delta) * RLen
        else:
            theta = delta * RLen
            RLen_min = ceil(RLen * delta)

        ## Ending Initialization ##
        t2 = time()
        init_time += t2-t1
        
        t1 = time()
        cands_scores = {}
        ## Starting Candidate Generation ##
        
        for pos_tok, (tok, tok_info) in enumerate(tokens):
            if theta - sum_stopped > 0.0000001:
                break
            
            if tok_info['utility'] == 0: #not qchunk
                continue
                
            sum_stopped -= tok_info['utility']

            if selfjoin:
                true_min = binary_search(lengths_list[tok], R)
                
                for S in lengths_list[tok][true_min:]:
                    
                    if R == S:
                        continue
                    
                    if len(collection2['words'][S]) > RLen_max:
                        break

                    if S not in cands_scores:
                        cands_scores[S] = 0
                    cands_scores[S] += tok_info['utility']
                # print("\t", pos_tok, tok, cands_scores)

            else:
                
                true_min = binary_search_dupl(lengths_list[tok], RLen, collection2['words'])
                for S in lengths_list[tok][true_min:]:
                    if len(collection2['words'][S]) > RLen_max:
                        break

                    if S not in cands_scores:
                        cands_scores[S] = 0
                    cands_scores[S] += tok_info['utility']
                
                for S in lengths_list[tok][true_min::-1]:
                    if len(collection2['words'][S]) < RLen_min:
                        break
    
                    if S not in cands_scores:
                        cands_scores[S] = 0
                    cands_scores[S] += tok_info['utility']        
                '''
                for S in lengths_list[tok]:
                    if RLen_min > len(collection2['words'][S]) > RLen_max:
                        continue

                    if S not in cands_scores:
                        cands_scores[S] = 0
                    cands_scores[S] += tok_info['utility']
                '''
                
        ## Ending Candidate Generation ##
        t2 = time()
        candgen_time += t2-t1
        no_candgen += len(cands_scores)
        
        ## Starting Candidate Refinement ##
        for S, util_gathered in cands_scores.items():
            t1 = time()
            (S_id, _) = collection2['collection'][S]
            S_rec = collection2['words'][S]
            SLen = len(S_rec)

            pers_delta = delta / (1.0 + delta) * (RLen + SLen);
            total = sum_stopped + util_gathered

            if pers_delta - total > .0000001:
                t2 = time()
                candref_time += t2-t1
                continue
                
            no_candref += 1                

            if jointFilter:
                UB = post_joint(R, S, tokens, idx, pers_delta, util_gathered, sum_stopped, pos_tok)
            elif posFilter:
                UB = post_positional(R, S, tokens, idx, pers_delta, util_gathered, sum_stopped, pos_tok)
            else:
                UB = post_basic(R, S, tokens, idx, pers_delta, total, pos_tok)

            if pers_delta - UB > .0000001:
                t2 = time()
                candref_time += t2-t1                
                continue

            no_candver += 1
            
            t1 = time()
            #score = verification(R_rec, S_rec)
            if RLen < SLen:
                score = verification(R_rec, S_rec, neds, pers_delta)
            else:
                score = verification(S_rec, R_rec, neds, pers_delta)
            t2 = time()
            candver_time += t2-t1

            if delta - score > 0.000000001:
                continue

            no_candres += 1
            output.append((R_id, S_id, score))

        ## Ending Candidate Refinement ##
        
        
    print('\nTime elapsed: Init: {:.2f}, Cand Gen: {:.2f}, Cand Ref: {:.2f}, Cand Ver: {:.2f}'.format(init_time, candgen_time, candref_time, candver_time))
    print('Candidates Generated: {:,}, Refined: {:,}, Verified: {:,}, Survived: {:,}'.format(no_candgen, no_candref, no_candver, no_candres))
        
    return output

class EditTokenJoin():
    
    #def tokenjoin(left_df, right_df, left_id, right_id, left_join, right_join, left_attr, right_attr, left_prefix='l_', right_prefix='r_'):
    def tokenjoin_self(self, df, id, join, attr=[], left_prefix='l_', right_prefix='r_', delta=0.7, jointFilter=False, posFilter=False):
        collection = transform_collection(df[join].values)
        idx, lengths_list = build_index(collection)
        
        output = simjoin(collection, collection, delta, 
                         idx, lengths_list, jointFilter, posFilter)
    
        output_df = pd.DataFrame(output, columns=[left_prefix+id, right_prefix+id, 'score'])
        for col in attr+[join, id]:
            output_df[left_prefix+col] = df.iloc[output_df[left_prefix+id]][col].values
        for col in attr+[join, id]:
            output_df[right_prefix+col] = df.iloc[output_df[right_prefix+id]][col].values    
        
        return output_df
    
    def tokenjoin_foreign(self, left_df, right_df, left_id, right_id, left_join, right_join, left_attr=[], right_attr=[], left_prefix='l_', right_prefix='r_', delta=0.7, jointFilter=False, posFilter=False):
        right_collection = transform_collection(right_df[right_join].values)
        idx, lengths_list = build_index(right_collection)
        
        left_collection = transform_collection(left_df[left_join].values, right_collection['dictionary'])
        
        output = simjoin(left_collection, right_collection,
                         delta, idx, lengths_list, jointFilter, posFilter)
        
        output_df = pd.DataFrame(output, columns=[left_prefix+left_id, right_prefix+right_id, 'score'])
        for col in left_attr+[left_join, left_id]:
            output_df[left_prefix+col] = left_df.iloc[output_df[left_prefix+left_id]][col].values
        for col in right_attr+[right_join, right_id]:
            output_df[right_prefix+col] = right_df.iloc[output_df[right_prefix+right_id]][col].values    
        
        return output_df

    def tokenjoin_prepare(self, right_df, right_id, right_join, right_attr=[], right_prefix='r_'):
        self.right_collection = transform_collection(right_df[right_join].values)
        self.idx, self.lengths_list = build_index(self.right_collection)
        self.right_df = right_df
        self.right_id = right_id
        self.right_join = right_join
        self.right_attr = right_attr
        self.right_prefix = right_prefix
        
    def tokenjoin_query(self, left_df, left_id, left_join, left_attr=[], left_prefix='l_', delta=0.7, jointFilter=False, posFilter=False):
        left_collection = transform_collection(left_df[left_join].values, self.right_collection['dictionary'])
        
        output = simjoin(left_collection, self.right_collection,
                         delta, self.idx, self.lengths_list, jointFilter, posFilter)
        
        output_df = pd.DataFrame(output, columns=[left_prefix+left_id, self.right_prefix+self.right_id, 'score'])
        for col in left_attr+[left_join, left_id]:
            output_df[left_prefix+col] = left_df.iloc[output_df[left_prefix+left_id]][col].values
        for col in self.right_attr+[self.right_join, self.right_id]:
            output_df[self.right_prefix+col] = self.right_df.iloc[output_df[self.right_prefix+self.right_id]][col].values    
        
        return output_df    

