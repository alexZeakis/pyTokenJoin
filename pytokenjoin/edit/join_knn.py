from math import floor, ceil
import pandas as pd
from time import time
from pytokenjoin.utils.verification import verification, verification_opt, neds
from pytokenjoin.utils.utils import binary_search, binary_search_dupl
from pytokenjoin.edit.join_utils import transform_collection, build_stats_for_record, build_index
import heapq

def simjoin(collection1, collection2, k, idx, lengths_list, log):

    selfjoin = collection1 == collection2
    delta = 0.000000001

    init_time = candgen_time = candref_time = candver_time = 0
    no_candgen = no_candref = no_candver = no_candres = 0
    output = []
    
    for R in range(len(collection1['words'])):
        
        temp_delta = delta
        temp_output = []
        
        if R % 100 == 0:
            print("\rProgress {:,}/{:,} \t: δ: {}".format(R, len(collection1['words']), temp_delta), end='')
            log[f'no_{R}'] = no_candres
        
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
        
        RLen_max = floor(RLen / temp_delta)
        
        if selfjoin:
            theta = 2 * temp_delta / (1 + temp_delta) * RLen
        else:
            theta = temp_delta * RLen
            RLen_min = ceil(RLen * temp_delta)

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
            
            if tok < 0:
                continue

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

            else:
                
                true_min = binary_search_dupl(lengths_list[tok], RLen, collection2['words'])
                for S in lengths_list[tok][true_min:]:
                    if len(collection2['words'][S]) > RLen_max:
                        break

                    if S not in cands_scores:
                        cands_scores[S] = 0
                    cands_scores[S] += tok_info['utility']
                
                true_min -= 1   # true_min examined in previous increasing parsing
                if true_min >= 0:    # reached start of inv list and -1 will go circular
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

        Q = []
        for S, util_gathered in cands_scores.items():
            
            total = sum_stopped + util_gathered
            (S_id, _) = collection2['collection'][S]
            S_rec = collection2['words'][S]
            SLen = len(S_rec)
            
            score = total / (RLen + SLen - total)
            
            if temp_delta - score > .0000001:
                continue
            heapq.heappush(Q, (-score, S, util_gathered, 0))
            
        ## Starting Candidate Refinement ##
        #for S, util_gathered in cands_scores.items():
        while len(Q) > 0 and -heapq.nsmallest(1, Q)[0][0] > temp_delta:
            (score, S, util_gathered, stage) = heapq.heappop(Q)    #heappop pops smallest, thus largest
            t1 = time()
            (S_id, _) = collection2['collection'][S]
            S_rec = collection2['words'][S]
            SLen = len(S_rec)
            # print()

            pers_delta = temp_delta / (1.0 + temp_delta) * (RLen + SLen)

            if stage == 0:
            
                no_candref += 1    
            
                total = sum_stopped + util_gathered
                if pers_delta - total > .0000001:
                    t2 = time()
                    candref_time += t2-t1
                    continue
                
                csum_stopped = sum_stopped
                for (tok, tok_info) in tokens[pos_tok:]:
                    csum_stopped -= tok_info['utility']
                    if tok in idx[S]:
                        tok_info_S = idx[S][tok]
                        util_gathered += tok_info['utility']
            
                        if pers_delta - (util_gathered + csum_stopped) > .0000001:
                            UB = util_gathered + csum_stopped
                            break
                        
                        if pers_delta - (util_gathered + tok_info_S['rest']) > .0000001:
                            UB = util_gathered + tok_info_S['rest']
                            break
                    else:
                        if pers_delta - (util_gathered + csum_stopped) > .0000001:
                            UB = util_gathered + csum_stopped
                            break
                else:
                    UB = util_gathered + csum_stopped
                    
                score = UB / (RLen + SLen - UB)
                
                if temp_delta - score < .0000001:
                    heapq.heappush(Q, (-score, S, util_gathered + csum_stopped, 1))
                t2 = time()
                candref_time += t2-t1                    
                    
            elif stage == 1:
                
                no_candver += 1
                
                if pers_delta - util_gathered > .0000001:   # delta might have changed
                    t2 = time()
                    candref_time += t2-t1
                    continue
                
                UB = util_gathered
                for (tok, tok_info) in tokens:
                    if tok in idx[S]:
                        UB -= tok_info['utility']
                        
                        tok_info_S = idx[S][tok]
                        minLen = min(len(tok_info['utilities_2']), len(tok_info_S['utilities_2'])) - 1
                        util_score = min(tok_info['utilities_2'][minLen], tok_info_S['utilities_2'][minLen])
                        UB += util_score
            
                    if pers_delta - total > .0000001:
                        break

                score = UB / (RLen + SLen - UB)
                if temp_delta - score < .0000001:
                    heapq.heappush(Q, (-score, S, UB, 2))
                t2 = time()
                candref_time += t2-t1  

            elif stage == 2: 
                
                no_candres += 1
                
                #score = verification(R_rec, S_rec)
                if RLen < SLen:
                    score = verification_opt(R_rec, S_rec, neds, pers_delta, 1)
                else:
                    score = verification_opt(S_rec, R_rec, neds, pers_delta, 1)

                if temp_delta - score > 0.000000001:
                    continue

                if score == 1.0:
                    continue
            
                if len(temp_output) < k:
                    heapq.heappush(temp_output, (score, R_id, S_id))
                else:
                    heapq.heappushpop(temp_output, (score, R_id, S_id))
                    temp_delta = heapq.nsmallest(1, temp_output)[0][0]
                    
                t2 = time()
                candver_time += t2-t1                    
            #temp_output.append((R_id, S_id, score))

            #print((R, S, score, R_id, S_id))
        ## Ending Candidate Refinement ##
        output += temp_output

    log['init_time'] = init_time
    log['candgen_time'] = candgen_time
    log['candref_time'] = candref_time
    log['candver_time'] = candver_time
    log['no_candgen'] = no_candgen
    log['no_candref'] = no_candref
    log['no_candver'] = no_candver
    log['no_candres'] = no_candres

    print('\nTime elapsed: Init: {:.2f}, Cand Gen: {:.2f}, Cand Ref: {:.2f}, Cand Ver: {:.2f}'.format(init_time, candgen_time, candref_time, candver_time))
    print('Candidates Generated: {:,}, Refined: {:,}, Verified: {:,}, Survived: {:,}, Final: {:,}'.format(no_candgen, no_candref, no_candver, no_candres, len(output)))
    # print('Final δ is {:.3f}'.format(delta))
    return output

class EditTokenJoin():
    
    def tokenjoin_self(self, df, id, join, attr=[], left_prefix='l_', right_prefix='r_', k=1000, keepLog=False):
        total_time = time()
        log = {}
        collection = transform_collection(df[join].values)
        idx, lengths_list = build_index(collection)
        
        output = simjoin(collection, collection, k, idx,
                         lengths_list, log)
        
        output_df = pd.DataFrame(output, columns=['score', left_prefix+id, right_prefix+id])
        for col in attr+[join, id]:
            #output_df[left_prefix+col] = df.set_index(id).loc[output_df[left_prefix+id], col].values
            output_df[left_prefix+col] = df.iloc[output_df[left_prefix+id]][col].values
        for col in attr+[join, id]:
            #output_df[right_prefix+col] = df.set_index(id).loc[output_df[right_prefix+id], col].values    
            output_df[right_prefix+col] = df.iloc[output_df[right_prefix+id]][col].values    
        output_df = output_df.sort_values('score', ascending=False)
        
        total_time = time() - total_time
        log['total_time'] = total_time
        if keepLog:
            return output_df, log
        return output_df
    
    
    def tokenjoin_foreign(self, left_df, right_df, left_id, right_id, left_join, right_join, left_attr=[], right_attr=[], left_prefix='l_', right_prefix='r_', k=1000, keepLog=False):
        total_time = time()
        log = {}
        right_collection = transform_collection(right_df[right_join].values)
        idx, lengths_list = build_index(right_collection)
        
        left_collection = transform_collection(left_df[left_join].values, right_collection['dictionary'])
        
        output = simjoin(left_collection, right_collection,
                         k, idx, lengths_list, log)
        
        output_df = pd.DataFrame(output, columns=['score', left_prefix+left_id, right_prefix+right_id])
        for col in left_attr+[left_join, left_id]:
            #output_df[left_prefix+col] = left_df.set_index(left_id).loc[output_df[left_prefix+left_id], col].values
            output_df[left_prefix+col] = left_df.iloc[output_df[left_prefix+left_id]][col].values
        for col in right_attr+[right_join, right_id]:
            #output_df[right_prefix+col] = right_df.set_index(right_id).loc[output_df[right_prefix+right_id], col].values    
            output_df[right_prefix+col] = right_df.iloc[output_df[right_prefix+right_id]][col].values  
        output_df = output_df.sort_values('score', ascending=False)
        
        total_time = time() - total_time
        log['total_time'] = total_time
        if keepLog:
            return output_df, log
        return output_df
    
    def tokenjoin_prepare(self, right_df, right_id, right_join, right_attr=[], right_prefix='r_'):
        self.right_collection = transform_collection(right_df[right_join].values)
        self.idx, self.lengths_list = build_index(self.right_collection)
        self.right_df = right_df
        self.right_id = right_id
        self.right_join = right_join
        self.right_attr = right_attr
        self.right_prefix = right_prefix
        
    
    def tokenjoin_query(self, left_df, left_id, left_join, left_attr=[], left_prefix='l_', k=1000, keepLog=False):
        log = {}
        left_collection = transform_collection(left_df[left_join].values, self.right_collection['dictionary'])
        
        output = simjoin(left_collection, self.right_collection,
                         k, self.idx, self.lengths_list, log)
        
        output_df = pd.DataFrame(output, columns=['score', left_prefix+left_id, self.right_prefix+self.right_id])
        for col in left_attr+[left_join, left_id]:
            output_df[left_prefix+col] = left_df.iloc[output_df[left_prefix+left_id]][col].values
        for col in self.right_attr+[self.right_join, self.right_id]:
            output_df[self.right_prefix+col] = self.right_df.iloc[output_df[self.right_prefix+self.right_id]][col].values    
        
        if keepLog:
            return output_df, log
        return output_df
