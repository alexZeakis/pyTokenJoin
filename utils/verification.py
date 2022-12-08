import networkx as nx
import networkx as nx
import editdistance

'''
def jaccard(r, s):
    rr = set(r)
    ss = set(s)
    return len(rr & ss) / len(rr | ss)
'''

def neds(r, s):
    return 1-editdistance.eval(r, s) / max((len(r), len(s)))

def jaccard(r, s):
    olap = pr = ps = 0
    maxr = len(r) - pr + olap;
    maxs = len(s) - ps + olap;

    while maxr > olap and maxs > olap :
        if r[pr] == s[ps] :
            pr += 1
            ps += 1
            olap += 1
        elif r[pr] < s[ps]: 
            pr += 1
            maxr -= 1
        else:
            ps += 1
            maxs -= 1

    return olap / (len(r) + len(s) - olap)


def deduplicate(r, s):
    olap = pr = ps = 0
    maxr = len(r) - pr + olap;
    maxs = len(s) - ps + olap;
    r_inds = []
    s_inds = []

    while maxr > olap and maxs > olap :
        if r[pr] == s[ps] :
            r_inds.append(pr)
            s_inds.append(ps)            
            pr += 1
            ps += 1
            olap += 1
        elif r[pr] < s[ps]: 
            pr += 1
            maxr -= 1
        else:
            ps += 1
            maxs -= 1

    return olap, set(r_inds), set(s_inds)


def verification(R_record, S_record, phi, pers_delta):
    
    # Start Element deduplication
    orRLen = len(R_record)
    orSLen = len(S_record)
    
    add, r_inds, s_inds = deduplicate(R_record, S_record)
    R_record = [r for no, r in enumerate(R_record) if no not in r_inds]
    S_record = [s for no, s in enumerate(S_record) if no not in s_inds]
    
    if (len(R_record)) == 0:
        score = add / (orRLen + orSLen - add)
        return score        
    # End Element deduplication
    '''
    add = 0
    '''
    
    UB = orRLen
    edges = []
    for nor, r in enumerate(R_record):
        max_NN = 0
        for nos, s in enumerate(S_record):
            score = phi(r, s)
            max_NN = max((max_NN, score))
            edges.append((f'r_{nor}', f's_{nos}', score))
            
        UB -= 1 - max_NN
        if pers_delta - UB > 0.0000001:
            return UB / (orRLen + orSLen - UB)
    #print(edges)

    G = nx.Graph()
    G.add_weighted_edges_from(edges)

    #print(G)
    #print(nx.max_weight_matching(G))

    matching = add
    for e in nx.max_weight_matching(G):
        matching += G.edges[e]['weight']

    score = matching / (orRLen + orSLen - matching)
    return score
