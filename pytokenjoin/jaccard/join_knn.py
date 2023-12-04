from math import floor, ceil
import pandas as pd
from time import time
import json
from pytokenjoin.utils.verification import jaccard, get_lower_bound, get_upper_bound
from pytokenjoin.utils.utils import binary_search, binary_search_dupl
from pytokenjoin.jaccard.join_utils import transform_collection, build_stats_for_record, build_index
import heapq


def delta_init(collection1, collection2, R, tokens, k, idx, 
               lengths_list, delta_alg, element,
               selfjoin=False, delta_generation=0.9, lamda=5):
    cached = set()
    output = []
    if delta_alg == 0:
        pass
    elif delta_alg == 1:
        R_id, R_set = collection1[R]
        for pos_tok, (tok, tok_info) in enumerate(tokens):
            if len(output) == k :
                break
            
            if tok < 0:
                continue

            for S in lengths_list[tok]:
                if selfjoin and R==S:
                    continue
                
                if S in cached:
                    continue
                
                S_id, S_set = collection2[S]
                if len(R_set) < len(S_set):
                    # score = verification_opt(R_set, S_set, jaccard, 0, 1)
                    score = get_upper_bound(R_set, S_set, jaccard)
                else:
                    # score = verification_opt(S_set, R_set, jaccard, 0, 1)
                    score = get_upper_bound(S_set, R_set, jaccard)
                cached.add(S)

                # if score == 0.0:
                    # continue
                
                heapq.heappush(output, (score, R_id, S_id))
                if len(output) > k:
                    heapq.heappop()
                    
                if len(output) == k and heapq.nsmallest(1, output)[0][0] > 0:
                    return output, cached
        # S = 0
        # while len(output) != k: # no common tokens, all others have score 0
        #     S_id, S_set = collection2[S]
        #     heapq.heappush(output, (0, R_id, S_id))
        #     S += 1

    elif delta_alg == 2:
        lamda = lamda * k
        
        
        R_id, R_set = collection1[R]
        RLen = len(R_set)
        # RLen_max = floor(RLen / delta_generation)
        sum_stopped = RLen
        if selfjoin:
            theta = 2 * delta_generation / (1 + delta_generation) * RLen
        else:
            theta = delta_generation * RLen
            # RLen_min = ceil(RLen * delta_generation)
        
        cands_scores = {}
        
        pos_tok = 0
        # while sum_stopped - theta > 0.0000001:
        while True:
            if theta - sum_stopped <= 0.0000001:
                if len(cands_scores) < lamda:   # if this delta gave not enough cands, many negative?
                    delta_generation -= 0.05
                    # RLen_max = floor(RLen / delta_generation)
                    if selfjoin:
                        theta = 2 * delta_generation / (1 + delta_generation) * RLen
                    else:
                        theta = delta_generation * RLen
                        # RLen_min = ceil(RLen * delta_generation)
                else:
                    break
            if pos_tok >= len(tokens):
                break
            (tok, tok_info) = tokens[pos_tok]
            pos_tok += 1
            sum_stopped -= tok_info['utility']
            
            if tok < 0:
                continue

            if selfjoin:
                true_min = binary_search(lengths_list[tok], R)

                for S in lengths_list[tok][true_min:]:
                    if R == S:
                        continue

                    # if len(collection2[S][1]) > RLen_max:
                    #     break

                    if S not in cands_scores:
                        cands_scores[S] = 0
                    cands_scores[S] += tok_info['utility']

            else:
                true_min = binary_search_dupl(lengths_list[tok], RLen, collection2)
                
                for S in lengths_list[tok][true_min:]:
                    
                    # if len(collection2[S][1]) > RLen_max:
                    #     break

                    if S not in cands_scores:
                        cands_scores[S] = 0
                    cands_scores[S] += tok_info['utility']
                
                
                true_min -= 1   # true_min examined in previous increasing parsing
                if true_min >= 0:    # reached start of inv list and -1 will go circular
                    for S in lengths_list[tok][true_min::-1]:
                        
                        # if len(collection2[S][1]) < RLen_min:
                        #     break
        
                        if S not in cands_scores:
                            cands_scores[S] = 0
                        cands_scores[S] += tok_info['utility']        
        
        Q = []
        for S, util_gathered in cands_scores.items():
            total = sum_stopped + util_gathered
            (S_id, S_rec) = collection2[S]
            SLen = len(S_rec)
            
            score = total / (RLen + SLen - total)
            
            #if delta_generation - score > .0000001:
            #    continue
            heapq.heappush(Q, (-score, S, util_gathered, 0))
            
        Q2 = []
        while len(Q) > 0:
            (score, S, util_gathered, stage) = heapq.heappop(Q)    #heappop pops smallest, thus largest
            (S_id, S_rec) = collection2[S]
            SLen = len(S_rec)      
        
            total = sum_stopped + util_gathered
            for (tok, tok_info) in tokens[pos_tok:]:
                total -= tok_info['utility']
                if tok in idx[S]:
                    total += tok_info['utility']
                
            score = total / (RLen + SLen - total)
            heapq.heappush(Q2, (-score, S))
            
        
        verified = 0
        threshold = 0.000000001
        ## Starting Candidate Refinement ##
        while len(Q2) > 0 and -heapq.nsmallest(1, Q2)[0][0] > threshold:
            (score, S) = heapq.heappop(Q2)    #heappop pops smallest, thus largest
            (S_id, S_rec) = collection2[S]
            SLen = len(S_rec)
            
            S_id, S_set = collection2[S]
            if len(R_set) < len(S_set):
                # score = verification_opt(R_set, S_set, jaccard, 0, 1)
                score = get_upper_bound(R_set, S_set, jaccard)
            else:
                # score = verification_opt(S_set, R_set, jaccard, 0, 1)
                score = get_upper_bound(S_set, R_set, jaccard)
            cached.add(S)

            # if score == 0.0:
                # continue
            
            if len(output) < k:
                heapq.heappush(output, (score, R_id, S_id))
            else:
                heapq.heappushpop(output, (score, R_id, S_id))
                
            if len(output) == k:    
                threshold = heapq.nsmallest(1, output)[0][0]                
                
            verified += 1
                
            if verified >= lamda:
                break
                
            if len(output) == k and heapq.nsmallest(1, output)[0][0] > 0:
                return output, cached

    return output, cached

def simjoin_basic(collection1, collection2, k, idx, lengths_list, delta_alg, log):

    # delta = 0.000000001
    
    output = []
    for R, (R_id, R_rec) in enumerate(collection1):
        # if R != 25:
        #     continue
        
        if R % 100 == 0:
            print("\rProgress {:,}/{:,}".format(R, len(collection1)), end='')

        temp_output = []
        for S, (S_id, S_rec) in enumerate(collection2):
            
            if len(R_rec) < len(S_rec):
                # score = verification_opt(R_rec, S_rec, jaccard, 0, 1)
                score = get_upper_bound(R_rec, S_rec, jaccard)
            else:
                # score = verification_opt(S_rec, R_rec, jaccard, 0, 1)
                score = get_upper_bound(S_rec, R_rec, jaccard)
        
            if score == 0:  #we do not want trivial results
                continue
        
            if len(temp_output) < k:
                heapq.heappush(temp_output, (score, R_id, S_id))
            else:
                heapq.heappushpop(temp_output, (score, R_id, S_id))

        output += temp_output

    return output


def simjoin_debug(collection1, collection2, k, idx, lengths_list, delta_alg, deltas, log):

    selfjoin = collection1 == collection2
    # delta = 0.000000001
    
    init_delta = init_time = candgen_time = candref_time = candver_time = 0
    candprior_time = candfilt_time = 0
    no_candgen = no_candref = no_candver = no_candres = 0
    output = []
    for R, (R_id, R_rec) in enumerate(collection1):
        
        # if R != 11:
        #     continue
        
        if R % 100 == 0:
            print("\rProgress {:,}/{:,}".format(R, len(collection1)), end='')
            log[f'no_{R}'] = no_candres

        t1 = time()
        ## Starting Initialization ##
        RLen = len(R_rec)
        
        if selfjoin:
            tokens = idx[R]
        else:
            tokens = build_stats_for_record(R_rec)
        
        tokens = sorted(tokens.items(), key=lambda x: x[0])
        sum_stopped = RLen
        
        
        temp_init_delta = time()
        #temp_output, cached = delta_init(collection1, collection2, R, tokens,
        #                                  k, idx, lengths_list, delta_alg, jaccard)
        temp_output = []
        cached = set()
        if R_id in deltas:
            temp_delta = deltas[R_id] - 0.000000001 # for marginal reasons
            # temp_delta = 0.000000001
        else:
            temp_delta = 0.000000001
        init_delta += time() - temp_init_delta
        
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
        
        pos_tok = 0
        while sum_stopped - theta > 0.000000001:
            if pos_tok >= len(tokens):
                break
            (tok, tok_info) = tokens[pos_tok]
            pos_tok += 1
            sum_stopped -= tok_info['utility']
            
            if tok < 0:
                continue

            if selfjoin:
                true_min = binary_search(lengths_list[tok], R)

                for S in lengths_list[tok][true_min:]:
                    if R == S or S in cached:
                    # if R == S:
                        continue

                    if len(collection2[S][1]) > RLen_max:
                        break

                    if S not in cands_scores:
                        cands_scores[S] = 0
                    cands_scores[S] += tok_info['utility']

            else:
                true_min = binary_search_dupl(lengths_list[tok], RLen, collection2)
                
                for S in lengths_list[tok][true_min:]:
                    if S in cached:
                        continue
                        
                    if len(collection2[S][1]) > RLen_max:
                        break

                    if S not in cands_scores:
                        cands_scores[S] = 0
                    cands_scores[S] += tok_info['utility']
                
                
                true_min -= 1   # true_min examined in previous increasing parsing
                if true_min >= 0:    # reached start of inv list and -1 will go circular
                    for S in lengths_list[tok][true_min::-1]:
                        if S in cached:
                            continue
                        
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

        t1 = time()
        Q = []
        for S, util_gathered in cands_scores.items():
            total = sum_stopped + util_gathered
            (S_id, S_rec) = collection2[S]
            SLen = len(S_rec)
            
            score = total / (RLen + SLen - total)
            
            # if S==245 or S==89:
            #     print(S, score, temp_delta)
            
            if temp_delta - score > .0000001:
                continue
            heapq.heappush(Q, (-score, S, util_gathered, 0))
        t2 = time()
        candprior_time += t2-t1        
        
        # print("Size is ", len(Q))
        
        t1 = time()
        ## Starting Candidate Refinement ##
        while len(Q) > 0 and -heapq.nsmallest(1, Q)[0][0] > temp_delta:
            (score, S, util_gathered, stage) = heapq.heappop(Q)    #heappop pops smallest, thus largest
            t11 = time()
            (S_id, S_rec) = collection2[S]
            SLen = len(S_rec)

            pers_delta = temp_delta / (1.0 + temp_delta) * (RLen + SLen)
            
            # if S == 245:
            #     print("Start ", stage, -score, temp_delta)

            if stage == 0:
            
                no_candref += 1    
            
                total = sum_stopped + util_gathered
                if pers_delta - total > .0000001:
                    t2 = time()
                    candfilt_time += t2-t11
                    # if S == 245:
                    #     print("End1 ", stage, -score, temp_delta)
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
                    heapq.heappush(Q, (-score, S, util_gathered, 1))
                    
                t2 = time()
                candfilt_time += t2-t11         
                # if S == 245:
                #     print("End2 ", 1, score, temp_delta)
                    
            elif stage == 1:
                
                no_candver += 1
                
                if pers_delta - util_gathered > .0000001:   # delta might have changed
                    t2 = time()
                    candfilt_time += t2-t11
                    # if S == 245:
                    #     print("End1 ", stage, -score, temp_delta)
                    continue
                
                UB = util_gathered
                for (tok, tok_info) in tokens:
                    if tok in idx[S]:
                        UB -= tok_info['utility']
                        
                        tok_info_S = idx[S][tok]
                        minLen = min(len(tok_info['utilities']), len(tok_info_S['utilities'])) - 1
                        util_score = min(tok_info['utilities'][minLen], tok_info_S['utilities'][minLen])
                        UB += util_score
            
                    if pers_delta - UB > .0000001:
                        break

                score = UB / (RLen + SLen - UB)
                if temp_delta - score < .0000001:
                    heapq.heappush(Q, (-score, S, UB, 2))
                t2 = time()
                candfilt_time += t2-t11  
                # if S == 245:
                #     print("End2 ", stage, score, temp_delta)

            
            elif stage == 2: 
                
                no_candres += 1
                # print("({}, {})".format(R, S))
                
                #score = verification(R_rec, S_rec)
                if RLen < SLen:
                    score = get_upper_bound(R_rec, S_rec, jaccard)
                else:
                    score = get_upper_bound(S_rec, R_rec, jaccard)

                if temp_delta - score > 0.000000001:
                    t2 = time()
                    candver_time += t2-t11  
                    continue

                
                #TODO: Fix this
                # if score == 1.0:
                #     continue
            
                if len(temp_output) < k:
                    heapq.heappush(temp_output, (score, R_id, S_id))
                else:
                    heapq.heappushpop(temp_output, (score, R_id, S_id))
                    
                if len(temp_output) == k:    
                    temp_delta = heapq.nsmallest(1, temp_output)[0][0]
                    
                t2 = time()
                candver_time += t2-t11                    
            #output.append((R_id, S_id, score))

        ## Ending Candidate Refinement ##
        output += temp_output
        t2 = time()
        candref_time += t2-t1   

    log['init_delta_time'] = init_delta
    log['init_time'] = init_time
    log['candgen_time'] = candgen_time
    log['candprior_time'] = candprior_time
    log['candref_time'] = candref_time
    log['candfilt_time'] = candfilt_time
    log['candver_time'] = candver_time
    log['no_candgen'] = no_candgen
    log['no_candref'] = no_candref
    log['no_candver'] = no_candver
    log['no_candres'] = no_candres

    print('\nTime elapsed: Init: {:.2f}, Cand Gen: {:.2f}, Cand Ref: {:.2f}, Cand Ver: {:.2f}'.format(init_time, candgen_time, candref_time, candver_time))
    print('Candidates Generated: {:,}, Refined: {:,}, Verified: {:,}, Survived: {:,}, Final: {:,}'.format(no_candgen, no_candref, no_candver, no_candres, len(output)))
    # print('Final δ is {:.3f}'.format(delta))
    return output


def simjoin(collection1, collection2, k, idx, lengths_list, delta_alg, log):

    selfjoin = collection1 == collection2
    # delta = 0.000000001
    
    init_delta = init_time = candgen_time = candref_time = candver_time = 0
    no_candgen = no_candref = no_candver = no_candres = 0
    output = []
    for R, (R_id, R_rec) in enumerate(collection1):
        
        # if R != 4:
        #     continue
        
        if R % 100 == 0:
            print("\rProgress {:,}/{:,}".format(R, len(collection1)), end='')
            log[f'no_{R}'] = no_candres

        t1 = time()
        ## Starting Initialization ##
        RLen = len(R_rec)
        
        if selfjoin:
            tokens = idx[R]
        else:
            tokens = build_stats_for_record(R_rec)
        
        tokens = sorted(tokens.items(), key=lambda x: x[0])
        sum_stopped = RLen
        
        
        temp_init_delta = time()
        temp_output, cached = delta_init(collection1, collection2, R, tokens,
                                          k, idx, lengths_list, delta_alg, jaccard)
        if len(temp_output) < k:
            temp_delta = 0.000000001
        else:
            temp_delta = heapq.nsmallest(1, temp_output)[0][0]
        init_delta += time() - temp_init_delta
        
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
        
        pos_tok = 0
        while sum_stopped - theta > 0.0000001:
            if pos_tok >= len(tokens):
                break
            (tok, tok_info) = tokens[pos_tok]
            pos_tok += 1
            sum_stopped -= tok_info['utility']
            
            if tok < 0:
                continue

            if selfjoin:
                true_min = binary_search(lengths_list[tok], R)

                for S in lengths_list[tok][true_min:]:
                    if R == S or S in cached:
                    # if R == S:
                        continue

                    if len(collection2[S][1]) > RLen_max:
                        break

                    if S not in cands_scores:
                        cands_scores[S] = 0
                    cands_scores[S] += tok_info['utility']

            else:
                true_min = binary_search_dupl(lengths_list[tok], RLen, collection2)
                
                for S in lengths_list[tok][true_min:]:
                    if S in cached:
                        continue
                        
                    if len(collection2[S][1]) > RLen_max:
                        break

                    if S not in cands_scores:
                        cands_scores[S] = 0
                    cands_scores[S] += tok_info['utility']
                
                
                true_min -= 1   # true_min examined in previous increasing parsing
                if true_min >= 0:    # reached start of inv list and -1 will go circular
                    for S in lengths_list[tok][true_min::-1]:
                        if S in cached:
                            continue
                        
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


        Q = []
        for S, util_gathered in cands_scores.items():
            total = sum_stopped + util_gathered
            (S_id, S_rec) = collection2[S]
            SLen = len(S_rec)
            
            score = total / (RLen + SLen - total)
            
            if temp_delta - score > .0000001:
                continue
            heapq.heappush(Q, (-score, S, util_gathered, 0))
            
        
        ## Starting Candidate Refinement ##
        while len(Q) > 0 and -heapq.nsmallest(1, Q)[0][0] > temp_delta:
            (score, S, util_gathered, stage) = heapq.heappop(Q)    #heappop pops smallest, thus largest
            t1 = time()
            (S_id, S_rec) = collection2[S]
            SLen = len(S_rec)

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
                    heapq.heappush(Q, (-score, S, util_gathered, 1))
                    
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
                        minLen = min(len(tok_info['utilities']), len(tok_info_S['utilities'])) - 1
                        util_score = min(tok_info['utilities'][minLen], tok_info_S['utilities'][minLen])
                        UB += util_score
            
                    if pers_delta - UB > .0000001:
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
                    # score = verification_opt(R_rec, S_rec, jaccard, pers_delta, 1)
                    score = get_upper_bound(R_rec, S_rec, jaccard)
                else:
                    # score = verification_opt(S_rec, R_rec, jaccard, pers_delta, 1)
                    score = get_upper_bound(S_rec, R_rec, jaccard)

                if temp_delta - score > 0.000000001:
                    continue

                
                #TODO: Fix this
                # if score == 1.0:
                #     continue
            
                if len(temp_output) < k:
                    heapq.heappush(temp_output, (score, R_id, S_id))
                else:
                    heapq.heappushpop(temp_output, (score, R_id, S_id))
                    
                if len(temp_output) == k:    
                    temp_delta = heapq.nsmallest(1, temp_output)[0][0]
                    
                t2 = time()
                candver_time += t2-t1                    
            #output.append((R_id, S_id, score))

        ## Ending Candidate Refinement ##
        output += temp_output

    log['init_delta_time'] = init_delta
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
    return output

class JaccardTokenJoin():
    
    def tokenjoin_self(self, df, id, join, attr=[], left_prefix='l_', right_prefix='r_', k=1000, delta_alg=0, keepLog=False):
        total_time = time()
        log = {}
        collection = transform_collection(df[join].values)
        idx, lengths_list = build_index(collection)
        
        output = simjoin(collection['collection'], collection['collection'], k, idx,
                         lengths_list, delta_alg, log)
        
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
    
    
    def tokenjoin_foreign(self, left_df, right_df, left_id, right_id, left_join, right_join, left_attr=[], right_attr=[], left_prefix='l_', right_prefix='r_', k=1000, delta_alg=0, keepLog=False, alg_run=0):
        total_time = time()
        log = {}
        right_collection = transform_collection(right_df[right_join].values)
        idx, lengths_list = build_index(right_collection)
        
        left_collection = transform_collection(left_df[left_join].values, right_collection['dictionary'])
        
        if alg_run == 0:
            output = simjoin(left_collection['collection'], right_collection['collection'],
                          k, idx, lengths_list, delta_alg, log)            
        elif alg_run == 1:
            output = simjoin_basic(left_collection['collection'], right_collection['collection'],                
                                   k, idx, lengths_list, delta_alg, log)            
        else:
            with open('D3_TokenJoin4_TRUE.txt') as f:
                for no, line in enumerate(f):
                    j = json.loads(line)
                    if j['k'] != k:
                        continue
                    deltas = {int(key): min(d) if len(d)==k else 0.000000001 for key, d in j['S_deltas'].items()}
                    print(len(deltas), len(left_collection['collection']), 
                          len(right_collection['collection']))
            output = simjoin_debug(left_collection['collection'], right_collection['collection'],                
                                   k, idx, lengths_list, delta_alg, deltas, log)            

        
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
        
    
    def tokenjoin_query(self, left_df, left_id, left_join, left_attr=[], left_prefix='l_', k=1000, delta_alg=0, keepLog=False):
        log = {}
        left_collection = transform_collection(left_df[left_join].values, self.right_collection['dictionary'])
        
        output = simjoin(left_collection['collection'], self.right_collection['collection'],
                         k, self.idx, self.lengths_list, delta_alg, log)
        
        output_df = pd.DataFrame(output, columns=['score', left_prefix+left_id, self.right_prefix+self.right_id])
        for col in left_attr+[left_join, left_id]:
            output_df[left_prefix+col] = left_df.iloc[output_df[left_prefix+left_id]][col].values
        for col in self.right_attr+[self.right_join, self.right_id]:
            output_df[self.right_prefix+col] = self.right_df.iloc[output_df[self.right_prefix+self.right_id]][col].values    
        
        if keepLog:
            return output_df, log
        return output_df       
