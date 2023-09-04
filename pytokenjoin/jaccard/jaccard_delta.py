from math import floor, ceil
import pandas as pd
from time import time
from pytokenjoin.utils.verification import verification, verification_opt, jaccard
from pytokenjoin.utils.utils import binary_search, binary_search_dupl, post_basic, post_positional
from pytokenjoin.jaccard.jaccard_utils import transform_collection, build_stats_for_record, build_index

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
            minLen = min(len(tok_info['utilities']), len(tok_info_S['utilities'])) - 1
            util_score = min(tok_info['utilities'][minLen], tok_info_S['utilities'][minLen])
            total += util_score

        if pers_delta - total > .0000001:
            return total
        
    return total    

def simjoin(collection1, collection2, delta, idx, lengths_list, jointFilter, posFilter, verification_alg):

    selfjoin = collection1 == collection2
    
    init_time = candgen_time = candref_time = candver_time = 0
    no_candgen = no_candref = no_candver = no_candres = 0
    output = []
    for R, (R_id, R_rec) in enumerate(collection1):
        
        if R % 100 == 0:
            print("\rProgress {:,}/{:,}".format(R, len(collection1)), end='')
        
        t1 = time()
        ## Starting Initialization ##
        RLen = len(R_rec)
        
        if selfjoin:
            tokens = idx[R]
        else:
            tokens = build_stats_for_record(R_rec)
        
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
                
            sum_stopped -= tok_info['utility']
            
            if tok < 0:
                continue

            if selfjoin:
                true_min = binary_search(lengths_list[tok], R)

                for S in lengths_list[tok][true_min:]:
                    if R == S:
                        continue

                    if len(collection2[S][1]) > RLen_max:
                        break

                    if S not in cands_scores:
                        cands_scores[S] = 0
                    cands_scores[S] += tok_info['utility']

            else:
                
                true_min = binary_search_dupl(lengths_list[tok], RLen, collection2)
                for S in lengths_list[tok][true_min:]:
                    if len(collection2[S][1]) > RLen_max:
                        break

                    if S not in cands_scores:
                        cands_scores[S] = 0
                    cands_scores[S] += tok_info['utility']
                
                true_min -= 1   # true_min examined in previous increasing parsing
                if true_min >= 0:    # reached start of inv list and -1 will go circular
                    for S in lengths_list[tok][true_min::-1]:
                        if len(collection2[S][1]) < RLen_min:
                            break
        
                        if S not in cands_scores:
                            cands_scores[S] = 0
                        cands_scores[S] += tok_info['utility']        
                '''
                for S in lengths_list[tok]:
                    if RLen_min > len(collection2[S][1]) > RLen_max:
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
            (S_id, S_rec) = collection2[S]
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
            if verification_alg >= 0:
                if RLen < SLen:
                    score = verification_opt(R_rec, S_rec, jaccard, pers_delta, verification_alg)
                else:
                    score = verification_opt(S_rec, R_rec, jaccard, pers_delta, verification_alg)            
            else:
                if RLen < SLen:
                    score = verification(R_rec, S_rec, jaccard, pers_delta)
                else:
                    score = verification(S_rec, R_rec, jaccard, pers_delta)
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

class JaccardTokenJoin():
    
    #def tokenjoin(left_df, right_df, left_id, right_id, left_join, right_join, left_attr, right_attr, left_prefix='l_', right_prefix='r_'):
    def tokenjoin_self(self, df, id, join, attr=[], left_prefix='l_', right_prefix='r_', delta=0.7, jointFilter=False, posFilter=False, verification_alg=0):
        collection = transform_collection(df[join].values)
        idx, lengths_list = build_index(collection)
        
        output = simjoin(collection['collection'], collection['collection'], delta, idx,
                         lengths_list, jointFilter, posFilter, verification_alg)
        
        output_df = pd.DataFrame(output, columns=[left_prefix+id, right_prefix+id, 'score'])
        for col in attr+[join, id]:
            #output_df[left_prefix+col] = df.set_index(id).loc[output_df[left_prefix+id], col].values
            output_df[left_prefix+col] = df.iloc[output_df[left_prefix+id]][col].values
        for col in attr+[join, id]:
            #output_df[right_prefix+col] = df.set_index(id).loc[output_df[right_prefix+id], col].values    
            output_df[right_prefix+col] = df.iloc[output_df[right_prefix+id]][col].values    
        
        return output_df
    
    
    def tokenjoin_foreign(self, left_df, right_df, left_id, right_id, left_join, right_join, left_attr=[], right_attr=[], left_prefix='l_', right_prefix='r_', delta=0.7, jointFilter=False, posFilter=False, verification_alg=0):
        right_collection = transform_collection(right_df[right_join].values)
        idx, lengths_list = build_index(right_collection)
        
        left_collection = transform_collection(left_df[left_join].values, right_collection['dictionary'])
        
        output = simjoin(left_collection['collection'], right_collection['collection'],
                         delta, idx, lengths_list, jointFilter, posFilter, verification_alg)
        
        output_df = pd.DataFrame(output, columns=[left_prefix+left_id, right_prefix+right_id, 'score'])
        for col in left_attr+[left_join, left_id]:
            #output_df[left_prefix+col] = left_df.set_index(left_id).loc[output_df[left_prefix+left_id], col].values
            output_df[left_prefix+col] = left_df.iloc[output_df[left_prefix+left_id]][col].values
        for col in right_attr+[right_join, right_id]:
            #output_df[right_prefix+col] = right_df.set_index(right_id).loc[output_df[right_prefix+right_id], col].values    
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
        
    
    def tokenjoin_query(self, left_df, left_id, left_join, left_attr=[], left_prefix='l_', delta=0.7, jointFilter=False, posFilter=False, verification_alg=0):
        left_collection = transform_collection(left_df[left_join].values, self.right_collection['dictionary'])
        
        output = simjoin(left_collection['collection'], self.right_collection['collection'],
                         delta, self.idx, self.lengths_list, jointFilter, posFilter, verification_alg)
        
        output_df = pd.DataFrame(output, columns=[left_prefix+left_id, self.right_prefix+self.right_id, 'score'])
        for col in left_attr+[left_join, left_id]:
            output_df[left_prefix+col] = left_df.iloc[output_df[left_prefix+left_id]][col].values
        for col in self.right_attr+[self.right_join, self.right_id]:
            output_df[self.right_prefix+col] = self.right_df.iloc[output_df[self.right_prefix+self.right_id]][col].values    
        
        return output_df        
