Seasort
=======

Sharded, asynchronous, external, and (not yet in-place) merge sort.

i.e.: merge sort for large files.

Aims
====

1. Lower bound for the number of assignments (write ops).

Strategy
========

N_CPU is the number of CPUs available for parallelizing work.

1. Distribute the file among CPUs (sharding):

   1. Divide the file in N_CPU parts (the boundaries must be block aligned).
   2. Start each CPU with a stream pointing to the start and going up to the
      end of the CPU's part in the file.


2. Single CPU work on a stream

   1. Divide the stream in:  total_file_size / total_mem_available parts.
   2. Do in-memory merge as much as possible.

Example of an 512GB file in a computer with 8GB of RAM and 4 CPUs::

    _________512GB___________
    |_A___:_B___:_C___:_D___|  4 shards of 128GB each
         /       \
        /         \
       /           \
      /             \
     /_____128GB_____\
    |__0__:.....:_63__:  64 partitions of 2GB each

Each CPU will be ordering at most 2GB at a time.

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
