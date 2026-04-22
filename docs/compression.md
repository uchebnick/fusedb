# FuseDB Compression

## 1. Purpose

This document describes compression strategies considered for FuseDB segments and blocks.

It covers:

- the unit of compression
- the separation between storage, segment format, and compression codec
- candidate compression approaches
- architectural trade-offs
- a phased recommendation

> [!IMPORTANT]
> This document is exploratory. It is not yet a frozen on-disk format specification.

---

## 2. Design Constraints

FuseDB targets:

- small keys and values
- frequent point reads
- frequent point updates
- hot keys and hot ranges
- immutable on-disk segments

Compression therefore must preserve:

- predictable point-read latency
- simple segment replacement
- clear ownership boundaries between modules
- the ability to evolve compression strategy over time

---

## 3. Compression Unit

FuseDB should treat the **block** as the unit of compression.

Recommended flow:

1. Build a raw logical block from ordered key/value entries.
2. Compress the whole raw block.
3. Store the compressed block in the segment file.
4. Use the segment index to locate the block.
5. Decompress the block on read, then search inside the raw block.

This keeps the design clean:

- `segment` owns block structure and index logic
- `compression` owns byte-to-byte transformation
- `disk` owns files, sync, rename, and read/write

Compression should not perform file IO directly.

---

## 4. Module Boundaries

Recommended responsibilities:

- `internal/disk`
  - filesystem abstraction
  - file creation/open/remove/rename
  - atomic replacement
- `internal/segment`
  - block format
  - block index
  - segment reader/builder
- `internal/compression`
  - block compression and decompression
  - dictionary training and runtime dictionary registry

Compression should operate on bytes:

- input: raw block bytes
- output: compressed block bytes

It should not know:

- leaf topology
- segment file layout
- filesystem details

---

## 5. Compression Strategies

### 5.1 No Compression

Store raw blocks exactly as written.

Pros:

- simplest implementation
- cheapest read path
- easiest debugging
- no dictionary lifecycle

Cons:

- higher disk usage
- higher IO per miss
- more cache pressure

Use when:

- bringing up the first correct segment format
- benchmarking a raw baseline

---

### 5.2 Prefix Compression

Compress adjacent sorted keys by storing only shared-prefix metadata plus suffix bytes.

Pros:

- natural fit for sorted blocks
- cheaper than heavy whole-block decompression
- good for repeated key prefixes
- predictable read cost

Cons:

- usually weaker ratio than whole-block dictionary compression
- helps keys much more than values
- adds block-internal decode complexity

Use when:

- key redundancy dominates
- point-read latency matters more than maximum compression ratio

---

### 5.3 Whole-Block Compression Without Dictionary

Build a raw block, then compress the entire block using a standard codec such as zstd without a custom dictionary.

Pros:

- simple conceptual model
- better ratio than raw blocks
- easy to benchmark
- no dictionary metadata lifecycle

Cons:

- every miss must decompress a full block
- lower ratio than a good dictionary on repetitive workloads

Use when:

- data is moderately compressible
- implementation simplicity is more important than maximum ratio

---

### 5.4 Whole-Block Compression With One Global Dictionary

Train one dictionary for the whole database and use it for all compressed blocks.

Pros:

- simple operational model
- small metadata surface
- easy recovery and GC
- usually good on repetitive, structured workloads

Cons:

- may fit poorly if data is highly heterogeneous
- retraining must be versioned carefully

Use when:

- starting dictionary compression
- data shapes are broadly similar across the database

Notes:

- dictionary size is usually small and roughly fixed
- typical sizes are tens of kilobytes, not proportional to total database size

---

### 5.5 Whole-Block Compression With Leaf-Group Dictionaries

Use one dictionary per group of leaves, for example one dictionary per approximately 16 leaves.

Pros:

- better local fit than a single global dictionary
- still bounded number of live dictionaries
- simpler than fully adaptive hierarchical schemes

Cons:

- requires mapping leaves to dictionary scopes
- split/merge handling becomes more complex
- more metadata than a single global dictionary

Use when:

- global dictionary quality is not good enough
- data varies by region of the tree

---

### 5.6 Adaptive Tree-Scoped Dictionaries

Bind dictionaries to tree nodes rather than to the whole database.

Candidate model:

- a node has an active dictionary
- a leaf merge may fork the nearest parent dictionary
- the fork becomes a candidate dictionary for that scope
- background training happens only when scheduler IO budget permits
- if the candidate is good enough, it becomes the new active dictionary for that scope
- later, scheduler may promote a fork upward and retrain it on the subtree visible at the higher node

Pros:

- adapts to local data distributions
- naturally creates different adaptation timescales
- lower scopes adapt quickly
- higher scopes become more stable and more general

Cons:

- highest architectural complexity
- needs strict lifecycle rules
- requires refcount-based GC for old dictionary versions
- difficult to debug without strong metrics
- can cause version explosion without bounded policies

This approach becomes reasonable only after the rest of the engine is stable.

---

#### 5.6.1 Core Idea

This approach treats compression dictionaries as **tree-scoped adaptive resources**.

Instead of having:

- one dictionary for the whole database
- or one dictionary per fixed leaf-group

the engine associates dictionaries with internal routing-tree nodes.

Each dictionary therefore has a natural **scope**:

- a low-level scope sees a small, local region of the key-space
- a higher-level scope sees a larger subtree
- lower scopes adapt faster
- higher scopes adapt slower and become more general

This creates a hierarchy of adaptation timescales:

- near leaves: fast adaptation, local fit
- near the root: slow adaptation, broad fit

---

#### 5.6.2 Scope Model

Every dictionary belongs to a tree node scope.

A scope owns:

- one active dictionary version
- zero or more candidate dictionary versions
- metadata describing version state and references

A scope can be thought of as:

- the subtree rooted at that node
- the set of leaves reachable through that node
- the data distribution visible from that part of the tree

Recommended identifiers:

- `scopeID`
- `dictID`
- `parentScopeID`
- `state`
- `baseDictID`

Useful states:

- `active`
- `candidate`
- `published`
- `retired`

An active dictionary is immutable and can be used by new segments.

A candidate dictionary is still being trained and evaluated. It is not used for foreground compression until published.

---

#### 5.6.3 Fork At Leaf-Merge Time

The entry point for adaptation is the leaf merge.

When a leaf finishes buffer-to-segment merge:

1. the leaf materializes a new immutable segment
2. the engine locates the nearest ancestor scope eligible for dictionary adaptation
3. the active dictionary of that scope is forked into a candidate
4. the candidate is scheduled for training if IO and CPU budget allow

The important detail is that the leaf does not immediately publish a new dictionary.

It only creates or requests a **candidate fork**.

That keeps foreground merge logic bounded:

- merge finishes
- segment is written
- dictionary work is delegated to the scheduler

This separation is important. Foreground write completion must not wait for dictionary training.

---

#### 5.6.4 Candidate Training

Candidate training happens in background and only when the scheduler decides budget is available.

Training corpus for the nearest scope should be representative, but bounded.

Examples:

- the newly written segment blocks from the triggering leaf
- recent blocks from sibling leaves in the same scope
- sampled blocks from the subtree rather than a full subtree scan

The candidate should be evaluated on:

- compression ratio gain
- compression CPU cost
- decompression CPU cost
- impact on miss-path latency

The candidate should be discarded if the gain is too small.

This avoids:

- version explosion
- wasted metadata
- useless dictionary churn

---

#### 5.6.5 Promotion Up The Tree

If a candidate becomes successful at one scope, the scheduler may later attempt to move the idea upward.

The upward step should work like this:

1. take a stable winning dictionary from a child or lower scope
2. fork the active dictionary of the parent scope
3. train the parent candidate on sampled data from all leaves routed through that parent node
4. evaluate the parent candidate against the current parent active dictionary
5. publish only if it wins by a meaningful margin

This is the key architectural property:

- lower scopes learn local structure first
- higher scopes learn only after there is evidence that the pattern generalizes

In other words, the tree does not blindly copy dictionaries upward.
It performs **promotion by validation**.

This makes the hierarchy self-regularizing:

- local noise tends to stay local
- only broad patterns survive upward

---

#### 5.6.6 Why Higher Levels Naturally Stabilize

As scope size grows, promotion becomes harder.

A higher-level scope:

- sees more heterogeneous data
- requires larger or more representative samples
- consumes more IO and CPU budget to train
- needs a stronger gain threshold to justify publication

Under sustained load, this means:

- low scopes may continue adapting
- parent scopes adapt less often
- sufficiently high scopes may almost stop changing

This is desirable.

It means the hierarchy naturally develops:

- hot and agile local dictionaries near leaves
- broad and stable dictionaries higher in the tree

The engine therefore gets adaptive behavior without requiring every level to retrain continuously.

Still, this stabilization should be intentional, not accidental.

The scheduler should explicitly control:

- promotion frequency
- training budget
- sample size
- minimum scope age before retraining
- minimum gain threshold for publication

---

#### 5.6.7 Suggested Scheduler Rules

Recommended rules:

1. a leaf merge may request a candidate fork, but never force training immediately
2. candidate training runs only when merge IO pressure is low enough
3. publication requires measurable improvement over the current active dictionary
4. upward promotion is rarer than local-scope training
5. root-near scopes should have the strictest thresholds and longest cooldowns

Useful gates:

- minimum number of blocks observed in scope
- minimum dictionary age before replacement
- minimum ratio improvement threshold
- maximum number of concurrent candidate trainings
- maximum number of live candidate versions per scope

Without these gates, the system will produce too many dictionary versions.

---

#### 5.6.8 Segment And Block Metadata

Segments and blocks must always remain readable even while dictionary versions evolve.

Each compressed block therefore needs a stable dictionary reference.

Minimum metadata:

- `dictID`
- `scopeID`
- `dictVersion`
- `rawLen`
- `compressedLen`

The segment does not need the full dictionary bytes embedded in every block.
It only needs the stable identifier used to resolve the dictionary at read time.

Recommended lookup:

- block header contains `dictID`
- reader resolves `dictID` through in-memory registry
- registry returns immutable dictionary runtime
- block is decompressed with that exact version

This guarantees that older segments remain readable even after newer dictionaries are published.

---

#### 5.6.9 Garbage Collection And Retention

Dictionary GC is mandatory in this design.

A dictionary version can be deleted only when:

- it is no longer active
- it is no longer a candidate
- no live segment references it

This implies refcount-like tracking:

- `dictID -> referenced segment count`

When a segment is replaced and deleted, its referenced dictionary count decreases.
When the count reaches zero and the dictionary is not active anywhere, the dictionary can be retired and removed.

This GC path must be crash-safe.

At restart, the engine should be able to rebuild dictionary references from persistent segment metadata.

---

#### 5.6.10 Interaction With Split And Merge

Tree topology changes must not invalidate dictionary ownership semantics.

Leaf split:

- old leaf disappears
- new leaves continue under whatever parent scope currently governs that subtree
- dictionaries do not need to be retrained immediately just because a split happened

Internal-node split or tree rebalancing:

- scope membership changes
- future candidate training should follow the new topology
- old segments continue using the dictionary version they were written with

This means dictionary scope should be treated as a versioned metadata concept, not as a mutable pointer baked into block contents.

---

#### 5.6.11 Strengths

This design gives:

- strong locality near leaves
- broad generalization higher in the tree
- natural adaptation timescales
- scheduler-controlled background evolution
- a path to handling heterogeneous datasets better than one global dictionary

It is especially attractive when:

- different key ranges develop different value shapes over time
- local hot ranges matter
- there is enough background budget to occasionally retrain candidates

---

#### 5.6.12 Risks

Main risks:

- too many dictionary versions
- too much scheduler work spent on training
- weak observability making it hard to explain behavior
- stale parent dictionaries if promotion is starved
- high implementation and recovery complexity

This approach should therefore be introduced only after the simpler designs are measured and understood.

---

## 6. Rejected Or Discouraged Strategies

### 6.1 Dictionary Per Block

Do not create a separate dictionary for each block.

Problems:

- extreme metadata growth
- terrible lifecycle complexity
- poor reuse
- expensive training for little gain

---

### 6.2 New Dictionary Version On Every Segment Mutation

Do not automatically publish a new dictionary on every segment rewrite.

Problems:

- too many versions
- difficult GC and recovery
- unstable compression behavior

---

### 6.3 File IO Inside Compression Module

Compression should not load dictionary files, open segment files, or perform rename/sync directly.

That responsibility belongs to storage and file modules.

---

## 7. Read-Path Implications

Compression affects misses more than hits.

Cold or uncached read path:

- index lookup
- block read from disk
- block decompression
- in-block key lookup

Hot read path depends on cache design:

- if cache stores compressed blocks, decompression still happens on hit
- if cache stores raw decompressed blocks, decompression cost disappears on hit
- if cache stores `key -> value`, exact hot-key hits avoid block work entirely

Implication:

- whole-block compression works best with either a raw block cache or a strong key/value cache

---

## 8. Dictionary Lifecycle Requirements

If dictionaries are versioned, the following invariants are required:

- every compressed block must reference a stable `dictID`
- active dictionaries are immutable
- old dictionaries remain readable while any segment still references them
- dictionary deletion requires zero remaining references
- retraining and publication happen in background only
- foreground reads and writes never block on dictionary training

Useful states:

- `active`
- `candidate`
- `published`
- `retired`

---

## 9. Recommended Phased Plan

### Phase 1

Use:

- raw blocks or plain whole-block compression
- at most one global dictionary

Reason:

- lowest complexity
- easiest benchmarking
- easiest recovery model

---

### Phase 2

Introduce:

- dictionary compression with one dictionary per leaf-group

Reason:

- captures local structure better
- still keeps dictionary count bounded

---

### Phase 3

Consider:

- adaptive tree-scoped dictionaries
- scheduler-driven promotion
- subtree training on sampled data

Only do this after:

- segment format is stable
- recovery is stable
- compression metrics exist
- dictionary GC exists

---

## 10. Current Recommendation

For FuseDB near term:

- keep compression as a pure byte codec module
- compress whole blocks, not whole segments
- keep file IO in `disk`
- keep block and index logic in `segment`
- start with either:
  - no compression
  - one global zstd dictionary

If the global dictionary later proves too coarse:

- move to one dictionary per leaf-group

Adaptive hierarchical dictionaries are a plausible future direction, but they should be introduced only after the engine core is stable and measurable.
