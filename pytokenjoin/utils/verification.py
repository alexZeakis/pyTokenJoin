import networkx as nx
import editdistance
import heapq

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

def verification_opt(R_record, S_record, phi, pers_delta, alg):
    
    # Start Element deduplication
    orRLen = len(R_record)
    orSLen = len(S_record)
    
    add, r_inds, s_inds = deduplicate(R_record, S_record)
    
    if alg == 2 and add - pers_delta > 0.0000001:
        return add / (orRLen + orSLen - add);

    R_record = [r for no, r in enumerate(R_record) if no not in r_inds]
    S_record = [s for no, s in enumerate(S_record) if no not in s_inds]
    
    if (len(R_record)) == 0:
        score = add / (orRLen + orSLen - add)
        return score        
    # End Element deduplication
    
    
    RLen = len(R_record)
    SLen = len(S_record)
    
    square = False
    if RLen == SLen: # square matrix
        colMin = [1.0 for _ in range(SLen)]
        square = True
    else:
        colMin = [0.0 for _ in range(SLen)]
        RLen = SLen # square matrix
        square = False
    
    UB = add + RLen
    nnEdges = [0 for _ in range(RLen)]
    hits2 = [[0 for _ in range(SLen)] for _ in range(RLen)]
    if alg == 2:
        pq = [[] for _ in range(RLen)]
    for nor, r in enumerate(R_record):
        for nos, s in enumerate(S_record):
            score = phi(r, s)
            nnEdges[nor] = max((nnEdges[nor], score))
            hits2[nor][nos] = score
    
            if alg == 2:
                heapq.heappush(pq[nor], (-score, nos))  #descending order to use pop
            
        UB -= 1 - nnEdges[nor]
        if pers_delta - UB > 0.0000001:
            return UB / (orRLen + orSLen - UB)    
    
    
    pi = [[0 for _ in range(SLen)] for _ in range(RLen)]
    
    # initialize: inverse and subtract row minima
    for r in range(RLen):
        for s in range(SLen):
            # print(len(pi), len(hits2), len(nnEdges), "\t", r, s, "\t", len(pi[r]), len(hits2[r]))
            pi[r][s] = nnEdges[r] - hits2[r][s]
            if square:
                colMin[s] = min((colMin[s], pi[r][s]))
    
    # initialize: subtract column minima
    if square:
        for s in range(SLen):
            if colMin[s] == 0: #there will be no change in this column
                continue
            for r in range(RLen):
                pi[r][s] = pi[r][s] - colMin[s]
    
    sumMatching = 0
    if alg == 0:
    	sumMatching = findMatching(pi, add, hits2)
    elif alg == 1:
     	sumMatching = findMatchingUB(pi, add, hits2, nnEdges, pers_delta)
    elif alg == 2:
     	sumMatching = findMatchingULB(pi, add, hits2, nnEdges, pers_delta, pq)
    score = sumMatching / (orRLen + orSLen - sumMatching)
    return score
    
    
def findMatching(pi, add, hits2):
    # Set<Edge> M = new HashSet<Edge>();
    M = set()
    rLen = len(pi)
    sLen = len(pi) # square matrix
    
    g = Graph(pi, rLen, sLen)
    
    ROffset = 0
    SOffset = rLen + ROffset
    sumMatching = 0
    
    while len(M) != rLen :
     	# find augmenting path
        pred = [0 for _ in range(g.V)]
        visited = [False for _ in range(g.V)]
    
        finalNode = g.BFS(g.src, g.dest, pred, visited)
    
        if finalNode == g.dest: # successful augmenting path
            crawl = pred[finalNode]
            P = set()
            while pred[crawl] != g.src:
                e = tuple(sorted((crawl, pred[crawl])))
                P.add(e)
                crawl = pred[crawl]
    
    		# search succesfful -> augment Matching
            M2 = set()
            for e in M:
                if e not in P:
                    M2.add(e)

            for e in P:
                if e not in M:
                    M2.add(e)

            for e in M:
                g.revertEdge(e[0], e[1]); # remove reversal of edges
            M = M2;
            sumMatching = add
            for e in M:
                g.updateEdge(e[0], e[1]);
                r = e[0] - ROffset
                s = e[1] - SOffset
                sumMatching += hits2[r][s]
    
        else: # landed in left Partite, we have a collision, variables need adjustment
    
    		# find delta
            delta = 1.1
    
    		# search delta (min pi) in Marked R -> Unmarked S
            for r in range(rLen):
                if visited[r]: # if R is marked
                    for s in range(sLen):
                        if not visited[s + SOffset]: # if S is unmarked
                            delta = min((delta, pi[r][s]))
    
    		# reduce Marked R -> Unmarked S, to enable more edges
            for r in range(rLen):
                if visited[r]:
                    for s in range(sLen):
                        if not visited[s + SOffset]: # if S is unmarked
                            pi[r][s] = pi[r][s] - delta
                            if pi[r][s] == 0.0:
                                g.addEdge(r, s)
    
    		# increase unmarked R -> marked S, to discourage colliding edges
            for r in range(rLen):
                if not visited[r]: # if R is unmarked
                    for s in range(sLen):
                        if visited[s + SOffset]: # if S is marked
                            pi[r][s] = pi[r][s] + delta
                            if pi[r][s] != 0.0:
                                g.removeEdge(r, s)
    
    return sumMatching;    

def findMatchingUB(pi, add, hits2, nnEdges, pers_delta):
    # Set<Edge> M = new HashSet<Edge>();
    M = set()
    rLen = len(pi)
    sLen = len(pi) # square matrix
    
    g = Graph(pi, rLen, sLen)
    
    ROffset = 0
    SOffset = rLen + ROffset
    sumMatching = 0
    
    while len(M) != rLen :
     	# find augmenting path
        pred = [0 for _ in range(g.V)]
        visited = [False for _ in range(g.V)]
    
        finalNode = g.BFS(g.src, g.dest, pred, visited)
    
        if finalNode == g.dest: # successful augmenting path
            crawl = pred[finalNode]
            P = set()
            while pred[crawl] != g.src:
                e = tuple(sorted((crawl, pred[crawl])))
                P.add(e)
                crawl = pred[crawl]
    
    		# search succesfful -> augment Matching
            M2 = set()
            for e in M:
                if e not in P:
                    M2.add(e)

            for e in P:
                if e not in M:
                    M2.add(e)

            for e in M:
                g.revertEdge(e[0], e[1]); # remove reversal of edges
            M = M2;
            sumMatching = add
            for e in M:
                g.updateEdge(e[0], e[1]);
                r = e[0] - ROffset
                s = e[1] - SOffset
                sumMatching += hits2[r][s]
                nnEdges[r] = hits2[r][s];

            UB = sumMatching
            for r in  g.adj[g.src]:
                UB += nnEdges[r]

            if pers_delta - UB > 0.0000001:
                return UB;
    
        else: # landed in left Partite, we have a collision, variables need adjustment
    
    		# find delta
            delta = 1.1
    
    		# search delta (min pi) in Marked R -> Unmarked S
            for r in range(rLen):
                if visited[r]: # if R is marked
                    for s in range(sLen):
                        if not visited[s + SOffset]: # if S is unmarked
                            delta = min((delta, pi[r][s]))
    
    		# reduce Marked R -> Unmarked S, to enable more edges
            for r in range(rLen):
                if visited[r]:
                    for s in range(sLen):
                        if not visited[s + SOffset]: # if S is unmarked
                            pi[r][s] = pi[r][s] - delta
                            if pi[r][s] == 0.0:
                                g.addEdge(r, s)
    
    		# increase unmarked R -> marked S, to discourage colliding edges
            for r in range(rLen):
                if not visited[r]: # if R is unmarked
                    for s in range(sLen):
                        if visited[s + SOffset]: # if S is marked
                            pi[r][s] = pi[r][s] + delta
                            if pi[r][s] != 0.0:
                                g.removeEdge(r, s)
    
    return sumMatching;  


def findMatchingULB(pi, add, hits2, nnEdges, pers_delta, pq):
    # Set<Edge> M = new HashSet<Edge>();
    M = set()
    rLen = len(pi)
    sLen = len(pi) # square matrix
    
    g = Graph(pi, rLen, sLen)
    
    ROffset = 0
    SOffset = rLen + ROffset
    sumMatching = 0
    
    LBEdges = [-1 for _ in range(rLen)] # store assignments for LB
    
    while len(M) != rLen :
     	# find augmenting path
        pred = [0 for _ in range(g.V)]
        visited = [False for _ in range(g.V)]
    
        finalNode = g.BFS(g.src, g.dest, pred, visited)
    
        if finalNode == g.dest: # successful augmenting path
            crawl = pred[finalNode]
            P = set()
            while pred[crawl] != g.src:
                e = tuple(sorted((crawl, pred[crawl])))
                P.add(e)
                crawl = pred[crawl]
    
    		# search succesfful -> augment Matching
            M2 = set()
            for e in M:
                if e not in P:
                    M2.add(e)

            for e in P:
                if e not in M:
                    M2.add(e)

            for e in M:
                g.revertEdge(e[0], e[1]); # remove reversal of edges
            M = M2;
            sumMatching = add
            for e in M:
                g.updateEdge(e[0], e[1]);
                r = e[0] - ROffset
                s = e[1] - SOffset
                sumMatching += hits2[r][s]
                nnEdges[r] = hits2[r][s];

            UB = sumMatching
            for r in g.adj[g.src]:
                UB += nnEdges[r]

            if pers_delta - UB > 0.0000001:
                return UB
                
            LB = sumMatching;
            for r in g.adj[g.src]:
            	# if pq[r] == None:  dummy left node
            	# 	continue;
            
                if len(g.adj[SOffset + LBEdges[r]]) == 0: # this LB is still free
                    continue

                while len(pq[r]) != 0 and len(g.adj[SOffset + heapq.nsmallest(1, pq[r])[0][1]]) > 0 : # iterate, until first best unmatched
                    heapq.heappop(pq[r])

                if len(pq[r]) != 0:
                    s_elem = heapq.nsmallest(1, pq[r])[0]
                    LB += -(s_elem[0])
                    LBEdges[r] = s_elem[1]
                
            if LB - pers_delta > 0.0000001:
                return LB
    
        else: # landed in left Partite, we have a collision, variables need adjustment
    
    		# find delta
            delta = 1.1
    
    		# search delta (min pi) in Marked R -> Unmarked S
            for r in range(rLen):
                if visited[r]: # if R is marked
                    for s in range(sLen):
                        if not visited[s + SOffset]: # if S is unmarked
                            delta = min((delta, pi[r][s]))
    
    		# reduce Marked R -> Unmarked S, to enable more edges
            for r in range(rLen):
                if visited[r]:
                    for s in range(sLen):
                        if not visited[s + SOffset]: # if S is unmarked
                            pi[r][s] = pi[r][s] - delta
                            if pi[r][s] == 0.0:
                                g.addEdge(r, s)
    
    		# increase unmarked R -> marked S, to discourage colliding edges
            for r in range(rLen):
                if not visited[r]: # if R is unmarked
                    for s in range(sLen):
                        if visited[s + SOffset]: # if S is marked
                            pi[r][s] = pi[r][s] + delta
                            if pi[r][s] != 0.0:
                                g.removeEdge(r, s)
    
    return sumMatching;  

class Graph:

	#int V; // No. of vertices
	#TIntSet adj[]; // Adjacency Lists
	#int ROffset, SOffset, totalOffset;
	#int src, dest;

    def __init__(self, e, rLen, sLen):
        self.ROffset = 0
        self.SOffset = rLen + self.ROffset
        totalOffset = sLen + self.SOffset

        self.V = rLen + sLen + 2; # R + S + (T, S)
        self.adj = [set() for _ in range(self.V)]

        self.dest = totalOffset;
        self.src = totalOffset + 1;

        for r in range(rLen):
            self.adj[self.src].add(r + self.ROffset)
            for s in range(sLen):
                if e[r][s] == 0.0:
                    self.adj[r + self.ROffset].add(s + self.SOffset)
                if len(self.adj[s + self.SOffset]) == 0:
                    self.adj[s + self.SOffset].add(self.dest)


    def addEdge(self, r, s):
        self.adj[r + self.ROffset].add(s + self.SOffset)

    def removeEdge(self, r, s):
        self.adj[r + self.ROffset].discard(s + self.SOffset)
        self.adj[s + self.SOffset].discard(r + self.ROffset) # could be in a matching already

    def updateEdge(self, esrc, edest):
        self.adj[self.src].discard(esrc)
        self.adj[esrc].discard(edest)
        self.adj[edest].add(esrc)
        self.adj[edest].discard(self.dest)

    def revertEdge(self, esrc, edest):
        self.adj[self.src].add(esrc)
        self.adj[esrc].add(edest)
        self.adj[edest].discard(esrc)
        self.adj[edest].add(self.dest)

	#a modified version of BFS that stores predecessor of each vertex in array pred
    def BFS(self, src, dest, pred, visited):
        queue = []

		# initially all vertices are unvisited so v[i] for all i is false and as no path is yet constructed
        for i in range(self.V):
            pred[i] = -1

		# now source is first to be visited and distance from source to itself should be 0
        visited[src] = True
        queue.append(src)

        u, v = -1, -1
		# bfs Algorithm
        while len(queue) != 0:
            u = queue.pop(0)

            for v in self.adj[u]:
                if visited[v] == False:
                    visited[v] = True
                    pred[v] = u
                    queue.append(v)

					# stopping condition (when we find our destination)
                    if v == dest:
                        return v

        if u < self.SOffset: # landed in Left Partite, we have a collision
            return u
        return -1


