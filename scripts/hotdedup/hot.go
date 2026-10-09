package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"math"
	"math/bits"
	"os"
	"sort"
)

const (
	infCost            = math.MaxInt / 4
	maxExactFiles      = 22
	defaultRootChoices = 32
)

type FileInput struct {
	Name   string   `json:"name"`
	Lambda float64  `json:"lambda"`
	Chunks []string `json:"chunks"`
}

type Input struct {
	EdgeNodes            []string    `json:"edge_nodes"`
	CapacityUniqueChunks int         `json:"capacity_unique_chunks"`
	Files                []FileInput `json:"files"`
}

type DeltaEdge struct {
	U, V int
	Cost int
}

type DeltaNeighbor struct {
	To   int
	Cost int
}

type DeltaGraph struct {
	FileUniq  []map[string]struct{}
	FileMu    []int
	FilePrize []int
	Edges     []DeltaEdge
	Adj       [][]DeltaNeighbor
}

type Solution struct {
	Selected []bool
	Cost     int
	Prize    int
}

func buildDeltaGraph(files []FileInput, prizeScale int) DeltaGraph {
	m := len(files)

	fileUniq := make([]map[string]struct{}, m)
	fileMu := make([]int, m)
	filePrize := make([]int, m)
	for i, f := range files {
		uniq := make(map[string]struct{}, len(f.Chunks))
		for _, c := range f.Chunks {
			uniq[c] = struct{}{}
		}
		fileUniq[i] = uniq
		fileMu[i] = len(uniq)

		prize := int(math.Ceil(f.Lambda * float64(prizeScale)))
		if prize < 0 {
			prize = 0
		}
		filePrize[i] = prize
	}

	chunkToFiles := make(map[string][]int)
	for i := range fileUniq {
		for c := range fileUniq[i] {
			chunkToFiles[c] = append(chunkToFiles[c], i)
		}
	}

	overlap := make([]map[int]int, m)
	for i := range overlap {
		overlap[i] = make(map[int]int)
	}
	for _, filesWithChunk := range chunkToFiles {
		if len(filesWithChunk) < 2 {
			continue
		}
		sort.Ints(filesWithChunk)
		for a := 0; a < len(filesWithChunk); a++ {
			for b := a + 1; b < len(filesWithChunk); b++ {
				i := filesWithChunk[a]
				j := filesWithChunk[b]
				overlap[i][j]++
			}
		}
	}

	edges := make([]DeltaEdge, 0)
	adj := make([][]DeltaNeighbor, m)
	for i := 0; i < m; i++ {
		for j, shared := range overlap[i] {
			if shared <= 0 {
				continue
			}
			edge := DeltaEdge{
				U:    i,
				V:    j,
				Cost: -shared,
			}
			edges = append(edges, edge)
			adj[i] = append(adj[i], DeltaNeighbor{To: j, Cost: edge.Cost})
			adj[j] = append(adj[j], DeltaNeighbor{To: i, Cost: edge.Cost})
		}
	}
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].Cost != edges[j].Cost {
			return edges[i].Cost < edges[j].Cost
		}
		if edges[i].U != edges[j].U {
			return edges[i].U < edges[j].U
		}
		return edges[i].V < edges[j].V
	})

	return DeltaGraph{
		FileUniq:  fileUniq,
		FileMu:    fileMu,
		FilePrize: filePrize,
		Edges:     edges,
		Adj:       adj,
	}
}

func buildSubsetTotals(values []int) []int {
	totalStates := 1 << len(values)
	out := make([]int, totalStates)
	for mask := 1; mask < totalStates; mask++ {
		lsb := mask & -mask
		idx := bits.TrailingZeros(uint(lsb))
		out[mask] = out[mask^lsb] + values[idx]
	}
	return out
}

type unionFind struct {
	parent []int
	rank   []int
}

func newUnionFind(n int) *unionFind {
	parent := make([]int, n)
	rank := make([]int, n)
	for i := range parent {
		parent[i] = i
	}
	return &unionFind{parent: parent, rank: rank}
}

func (uf *unionFind) find(x int) int {
	for uf.parent[x] != x {
		uf.parent[x] = uf.parent[uf.parent[x]]
		x = uf.parent[x]
	}
	return x
}

func (uf *unionFind) union(a, b int) bool {
	ra := uf.find(a)
	rb := uf.find(b)
	if ra == rb {
		return false
	}
	if uf.rank[ra] < uf.rank[rb] {
		ra, rb = rb, ra
	}
	uf.parent[rb] = ra
	if uf.rank[ra] == uf.rank[rb] {
		uf.rank[ra]++
	}
	return true
}

func mstCostForMask(mask uint32, numFiles int, edges []DeltaEdge) (int, bool) {
	nodeCount := bits.OnesCount32(mask)
	if nodeCount <= 1 {
		return 0, true
	}

	uf := newUnionFind(numFiles)
	needEdges := nodeCount - 1
	usedEdges := 0
	totalCost := 0

	for _, e := range edges {
		uBit := uint32(1) << e.U
		vBit := uint32(1) << e.V
		if mask&uBit == 0 || mask&vBit == 0 {
			continue
		}
		if uf.union(e.U, e.V) {
			totalCost += e.Cost
			usedEdges++
			if usedEdges == needEdges {
				return totalCost, true
			}
		}
	}

	return 0, false
}

func mstCostForSelection(selected []bool, edges []DeltaEdge) (int, bool) {
	selectedCount := 0
	for _, ok := range selected {
		if ok {
			selectedCount++
		}
	}
	if selectedCount <= 1 {
		return 0, true
	}

	uf := newUnionFind(len(selected))
	needEdges := selectedCount - 1
	usedEdges := 0
	totalCost := 0
	for _, e := range edges {
		if !selected[e.U] || !selected[e.V] {
			continue
		}
		if uf.union(e.U, e.V) {
			totalCost += e.Cost
			usedEdges++
			if usedEdges == needEdges {
				return totalCost, true
			}
		}
	}
	return 0, false
}

func selectedFromMask(mask uint32, n int) []bool {
	selected := make([]bool, n)
	for i := 0; i < n; i++ {
		selected[i] = mask&(uint32(1)<<i) != 0
	}
	return selected
}

func solveQuotaExact(graph DeltaGraph, targetQ int, subsetMu, subsetPrize []int) (Solution, bool) {
	bestMask := uint32(0)
	best := Solution{Cost: infCost}
	found := false

	for mask := 0; mask < len(subsetPrize); mask++ {
		if subsetPrize[mask] < targetQ {
			continue
		}

		mstCost, ok := mstCostForMask(uint32(mask), len(graph.FilePrize), graph.Edges)
		if !ok {
			continue
		}
		totalCost := subsetMu[mask] + mstCost
		totalPrize := subsetPrize[mask]

		if !found || totalCost < best.Cost || (totalCost == best.Cost && totalPrize > best.Prize) {
			best = Solution{
				Cost:  totalCost,
				Prize: totalPrize,
			}
			bestMask = uint32(mask)
			found = true
		}
	}

	if found {
		best.Selected = selectedFromMask(bestMask, len(graph.FilePrize))
	}
	return best, found
}

func solveQuotaApprox(graph DeltaGraph, targetQ int, rootChoices int) (Solution, bool) {
	if len(graph.FilePrize) == 0 {
		return Solution{}, false
	}

	type rootCandidate struct {
		idx   int
		prize int
	}
	candidates := make([]rootCandidate, 0, len(graph.FilePrize))
	for i, prize := range graph.FilePrize {
		if prize > 0 {
			candidates = append(candidates, rootCandidate{idx: i, prize: prize})
		}
	}
	if len(candidates) == 0 {
		return Solution{}, false
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].prize != candidates[j].prize {
			return candidates[i].prize > candidates[j].prize
		}
		return graph.FileMu[candidates[i].idx] < graph.FileMu[candidates[j].idx]
	})
	if rootChoices <= 0 {
		rootChoices = defaultRootChoices
	}
	if rootChoices > len(candidates) {
		rootChoices = len(candidates)
	}

	best := Solution{Cost: infCost}
	found := false

	for _, root := range candidates[:rootChoices] {
		selected := make([]bool, len(graph.FilePrize))
		bestLinkCost := make([]int, len(graph.FilePrize))
		for i := range bestLinkCost {
			bestLinkCost[i] = infCost
		}

		selected[root.idx] = true
		currentPrize := graph.FilePrize[root.idx]
		for _, nb := range graph.Adj[root.idx] {
			if nb.Cost < bestLinkCost[nb.To] {
				bestLinkCost[nb.To] = nb.Cost
			}
		}

		for currentPrize < targetQ {
			bestNode := -1
			bestRatio := math.Inf(1)
			bestMarginal := infCost
			bestNodePrize := -1

			for i, prize := range graph.FilePrize {
				if prize <= 0 || selected[i] {
					continue
				}
				linkCost := bestLinkCost[i]
				if linkCost == infCost {
					continue
				}
				marginalCost := graph.FileMu[i] + linkCost
				ratio := float64(marginalCost) / float64(prize)
				if bestNode == -1 ||
					ratio < bestRatio ||
					(ratio == bestRatio && marginalCost < bestMarginal) ||
					(ratio == bestRatio && marginalCost == bestMarginal && prize > bestNodePrize) {
					bestNode = i
					bestRatio = ratio
					bestMarginal = marginalCost
					bestNodePrize = prize
				}
			}

			if bestNode == -1 {
				break
			}

			selected[bestNode] = true
			currentPrize += graph.FilePrize[bestNode]
			for _, nb := range graph.Adj[bestNode] {
				if nb.Cost < bestLinkCost[nb.To] {
					bestLinkCost[nb.To] = nb.Cost
				}
			}
		}

		if currentPrize < targetQ {
			continue
		}

		muCost := 0
		for i, ok := range selected {
			if ok {
				muCost += graph.FileMu[i]
			}
		}
		mstCost, ok := mstCostForSelection(selected, graph.Edges)
		if !ok {
			continue
		}
		totalCost := muCost + mstCost
		if !found || totalCost < best.Cost || (totalCost == best.Cost && currentPrize > best.Prize) {
			best = Solution{
				Selected: append([]bool(nil), selected...),
				Cost:     totalCost,
				Prize:    currentPrize,
			}
			found = true
		}
	}

	return best, found
}

func dstpSelectExact(graph DeltaGraph, budget int) (Solution, int, bool) {
	subsetMu := buildSubsetTotals(graph.FileMu)
	subsetPrize := buildSubsetTotals(graph.FilePrize)
	totalPrize := subsetPrize[len(subsetPrize)-1]

	best, ok := solveQuotaExact(graph, 0, subsetMu, subsetPrize)
	if !ok {
		return Solution{}, 0, false
	}

	low, high := 0, totalPrize
	bestQuota := 0
	for low < high {
		mid := (low + high + 1) / 2
		sol, ok := solveQuotaExact(graph, mid, subsetMu, subsetPrize)
		if ok && sol.Cost <= budget {
			low = mid
			best = sol
			bestQuota = mid
		} else {
			high = mid - 1
		}
	}

	if low != bestQuota {
		best, ok = solveQuotaExact(graph, low, subsetMu, subsetPrize)
		if !ok || best.Cost > budget {
			return Solution{}, 0, false
		}
		bestQuota = low
	}

	return best, bestQuota, true
}

func dstpSelectApprox(graph DeltaGraph, budget int, rootChoices int) (Solution, int, bool) {
	totalPrize := 0
	for _, prize := range graph.FilePrize {
		totalPrize += prize
	}
	if totalPrize <= 0 {
		return Solution{}, 0, false
	}

	best, ok := solveQuotaApprox(graph, 0, rootChoices)
	if !ok {
		return Solution{}, 0, false
	}

	low, high := 0, totalPrize
	bestQuota := 0
	for low < high {
		mid := (low + high + 1) / 2
		sol, ok := solveQuotaApprox(graph, mid, rootChoices)
		if ok && sol.Cost <= budget {
			low = mid
			best = sol
			bestQuota = mid
		} else {
			high = mid - 1
		}
	}

	if low != bestQuota {
		best, ok = solveQuotaApprox(graph, low, rootChoices)
		if !ok || best.Cost > budget {
			return Solution{}, 0, false
		}
		bestQuota = low
	}

	return best, bestQuota, true
}

func hash64(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

func jumpConsistentHash(key uint64, numBuckets int) int {
	var b int64 = -1
	var j int64
	for j < int64(numBuckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(1<<31) / float64((key>>33)+1)))
	}
	return int(b)
}

type ChunkRef struct {
	Index int    `json:"index"`
	Hash  string `json:"hash"`
	Node  string `json:"node"`
}

type Output struct {
	SelectedFiles  []string              `json:"selected_files"`
	BestQuotaInt   int                   `json:"best_quota_int"`
	EstimatedCost  int                   `json:"estimated_cost"`
	UniqueChunks   int                   `json:"unique_chunks"`
	ChunkPlacement map[string]string     `json:"chunk_placement"`
	FileRecipes    map[string][]ChunkRef `json:"file_recipes"`
	NodeChunks     map[string][]string   `json:"node_chunks"`
}

func main() {
	inPath := flag.String("in", "", "input json path")
	outPath := flag.String("out", "", "output json path (optional; default stdout)")
	prizeScale := flag.Int("prizeScale", 1000, "scale lambda to integer prize using ceil(lambda*scale)")
	rootChoices := flag.Int("roots", defaultRootChoices, "number of high-prize root candidates for large-instance approximation")
	flag.Parse()

	if *inPath == "" {
		fmt.Fprintln(os.Stderr, "usage: go run hot.go -in input.json [-out out.json] [-prizeScale 1000]")
		os.Exit(1)
	}
	if *prizeScale <= 0 {
		panic("prizeScale must be > 0")
	}

	raw, err := os.ReadFile(*inPath)
	if err != nil {
		panic(err)
	}

	var in Input
	if err := json.Unmarshal(raw, &in); err != nil {
		panic(err)
	}
	if len(in.EdgeNodes) == 0 || len(in.Files) == 0 || in.CapacityUniqueChunks <= 0 {
		panic("edge_nodes/files/capacity_unique_chunks invalid")
	}
	graph := buildDeltaGraph(in.Files, *prizeScale)
	var (
		best      Solution
		bestQuota int
		ok        bool
	)
	if len(in.Files) <= maxExactFiles {
		best, bestQuota, ok = dstpSelectExact(graph, in.CapacityUniqueChunks)
	} else {
		best, bestQuota, ok = dstpSelectApprox(graph, in.CapacityUniqueChunks, *rootChoices)
	}
	if !ok {
		panic("no feasible solution found under the delta-similarity quota model")
	}

	selectedIdx := make([]int, 0)
	for i := range in.Files {
		if best.Selected[i] && graph.FilePrize[i] > 0 {
			selectedIdx = append(selectedIdx, i)
		}
	}

	selectedNames := make([]string, 0, len(selectedIdx))
	for _, idx := range selectedIdx {
		selectedNames = append(selectedNames, in.Files[idx].Name)
	}
	sort.Strings(selectedNames)

	uniqChunks := make(map[string]struct{})
	for _, idx := range selectedIdx {
		for c := range graph.FileUniq[idx] {
			uniqChunks[c] = struct{}{}
		}
	}

	chunkPlacement := make(map[string]string, len(uniqChunks))
	nodeChunks := make(map[string][]string, len(in.EdgeNodes))
	for _, node := range in.EdgeNodes {
		nodeChunks[node] = []string{}
	}

	chunksList := make([]string, 0, len(uniqChunks))
	for c := range uniqChunks {
		chunksList = append(chunksList, c)
	}
	sort.Strings(chunksList)

	for _, c := range chunksList {
		bucket := jumpConsistentHash(hash64(c), len(in.EdgeNodes))
		node := in.EdgeNodes[bucket]
		chunkPlacement[c] = node
		nodeChunks[node] = append(nodeChunks[node], c)
	}
	for node := range nodeChunks {
		sort.Strings(nodeChunks[node])
	}

	fileRecipes := make(map[string][]ChunkRef)
	for _, idx := range selectedIdx {
		f := in.Files[idx]
		refs := make([]ChunkRef, 0, len(f.Chunks))
		for pos, c := range f.Chunks {
			refs = append(refs, ChunkRef{
				Index: pos,
				Hash:  c,
				Node:  chunkPlacement[c],
			})
		}
		fileRecipes[f.Name] = refs
	}

	out := Output{
		SelectedFiles:  selectedNames,
		BestQuotaInt:   bestQuota,
		EstimatedCost:  best.Cost,
		UniqueChunks:   len(uniqChunks),
		ChunkPlacement: chunkPlacement,
		FileRecipes:    fileRecipes,
		NodeChunks:     nodeChunks,
	}

	encoded, _ := json.MarshalIndent(out, "", "  ")
	if *outPath == "" {
		fmt.Println(string(encoded))
		return
	}
	if err := os.WriteFile(*outPath, encoded, 0644); err != nil {
		panic(err)
	}
}
