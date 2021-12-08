Seasort
=======

Sharded, asynchronous, external, and (not yet in-place) merge sort.

i.e.: merge sort for large files.


Aims
====

1. Lower bound for the number of assignments (write ops).


Strategy
========

Sharding
   The number of shards is equal to the number of CPUs available. The
   shards run parallel (simultaneous) tasks and do not share anything,
   following the shared-nothing design from Seastar library.

Sectioning
   A single shard is logically divided into N sections, which are
   continuously & asynchronously swapped to/from M slots preallocated
   in memory. M should generally be much smaller than N.

The diagram below illustrates what was defined::

    _______________ _______________ _______________ _______________
   |    Shard 0    |    Shard 1    |    Shard 2    |    Shard 3    |  64GB (file size)
   |_______________|_______________|_______________|_______________|
    _ _ _ _ _ _ _-´                 `-_ _ _ _ _ _ _ _ _ _ _ _ _ _ _
   ´_______ _______ _______ _______ _______ _______ _______ _______`
   | Sec 0 | Sec 1 | Sec 2 | Sec 3 | Sec 4 | Sec 5 | Sec 6 | Sec 7 |  32GB (shard size)
   |_______|_______|_______|_______|_______|_______|_______|_______|
    _ _ _ _ _ _ _-´         `-_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
   ´___ ___ ___ ___ ___ ___ ___ ___ ___ ___ _______________________`
   | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 |         . . .         |   2GB (section size)
   |___|___|___|___|___|___|___|___|___|___|_______________________|

M = available_memory / section_size

TODO: Fix some of the descriptions below.

An optimal number of slots M will prevent the CPU from idling/waiting
for a buffered read operation before start merging. In other words, this
allows the CPU to work while the disk is fetching data. The number of
sections is not defined by the program because there must be a balance
between the overheads from disk access and cpu merge speed.

The loop of a single CPU is:

1. Request read of (M - pending) sections from disk.
2. Wait for a single section to be ready.
3. Merge section.
4. Request write of the merged section to disk.
5. Go to 1.

Based on the above scenario, the following assumptions are made:

- A shard may be much larger than the RAM available to merge it.
- Each CPU will be ordering at most M sections at a time.

I.e. A Binary Merge Tree (BMT) for parallel merging
---------------------------------------------------

There are two steps involved::

            ___|__|___
           /          \
      ___|__|___       \____
     /          \           \
   |__|        |__|        |__|
  /    \      /    \      /    \
|__|  |__|  |__|  |__|  |__|  |__|

1. Merge blocks from one shard.
2. Merge blocks from two shards.

The first step is where the maximum parellelism is achieved. Every cpu is
merging blocks from its own shard.


Merge sections
==============

For example, if there are 2GB of memory available for each shard, a merge
section might be 500MB -- and the shard will have 4 merge sections.

Merge sections are constant in size.

4 parallel reads are issued to disk. Each time a merge section is fully merged,
a new read is issued for the next section.


Further improvements
===============================================

- in-place merge


Background
==========

Relevant topics:

- merge sort (algorithm)
- stable sort (behavior of a sort algorithm)
- in-place merge (behavior of a merge algorithm)
- external merge (locality of a merge algorithm)
- k-way merge (strategy of a merge algorithm)

Relevant papers:

.. [MJCS2005] Islam, Md. Rafiqul and S. M. Raquib Uddin. “AN EXTERNAL SORTING
   ALGORITHM USING IN-PLACE MERGING AND WITH NO ADDITIONAL DISK SPACE.”
   Malaysian Journal of Computer Science 18 (2005): 40-49.
   <https://api.semanticscholar.org/CorpusID:51807352>
.. [SOFSEM2007] Kim, Pok-Son and Arne Kutzner. “A Simple Algorithm for Stable
   Minimum Storage Merging.” SOFSEM (2007).
   <https://api.semanticscholar.org/CorpusID:8875129>
.. [TAMC2008] Kim, Pok-Son and Arne Kutzner. “Ratio Based Stable In-Place
   Merging.” TAMC (2008).
   <https://api.semanticscholar.org/CorpusID:10161373>
.. [CS2020] SalahAhmad, LiKenli, LiaoQing, HashemMervat, LiZhiyong, T.
   ChronopoulosAnthony and Y. ZomayaAlbert. “A Time-space Efficient Algorithm
   for Parallel k-way In-place Merging based on Sequence Partitioning and
   Perfect Shuffle.” (2020).
   <https://api.semanticscholar.org/CorpusID:225897261>
