/*
 * Copyright 2010-2016, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#include "small.h"
#include <assert.h>
#include <string.h>
#include <stdio.h>

enum {
	POOL_PER_GROUP_MAX = 32,
};

/**
 * Calculates last pool in group.
 * In a group of pools with the same pool size,
 * there can be no more than 32 pools
 * @param[in] first - first pool in new group.
 * @param[in] last - last pool with same slab_size.
 * @return last small pool in group.
 */
static inline struct small_mempool*
calculate_last_pool_in_group(struct small_mempool *first,
			     struct small_mempool *last)
{
	uint32_t c = 0;
	struct small_mempool *last_in_group = first;
	while (last_in_group < last && (c++) < POOL_PER_GROUP_MAX - 1)
		last_in_group++;
	return last_in_group;
}

/**
 * Creates new small pool group. First assigns all the pools
 * in the group their indexes, then mark last pool in group as
 * available for allocation. From now all mempools in group will
 * allocate memory from it, as long as their waste < waste_max.
 * @param[in] first -first pool in group.
 * @param[in] last - last pool in group.
 */
static inline void
small_mempool_create_group(struct small_alloc *alloc,
			   struct small_mempool *first,
			   struct small_mempool *last)
{
	struct small_mempool_group *group =
		&alloc->small_mempool_groups[alloc->small_mempool_groups_size];
	while (first <= last) {
		first->group = group;
		first++;
	}
	group->non_optimal_alloc = 0;
	group->non_optimal_alloc_max = last->pool.objcount;
	group->last_in_group = last;
	++alloc->small_mempool_groups_size;
}

/**
 * Creates one or more groups of pools from pools with the same slab size.
 * (One group for 32 or less pools).
 * @param[in] first - First pool with same slab size.
 * @param[in] last - Last pool with same slab size.
 */
static inline void
small_mempool_create_groups(struct small_alloc *alloc,
			    struct small_mempool *first,
			    struct small_mempool *last)
{
	struct small_mempool *first_in_group = first;
	while (first_in_group <= last) {
		struct small_mempool *last_in_group =
			calculate_last_pool_in_group(first_in_group, last);
		small_mempool_create_group(alloc, first_in_group, last_in_group);
		first_in_group = last_in_group + 1;
	}
}

static inline struct small_mempool *
small_mempool_search(struct small_alloc *alloc, size_t size)
{
	if (size > alloc->objsize_max)
		return NULL;
	unsigned cls =
		small_class_calc_offset_by_size(&alloc->small_class, size);
	struct small_mempool *pool = &alloc->small_mempool_cache[cls];
	return pool;
}

static inline void
small_mempool_create(struct small_alloc *alloc)
{
	uint32_t slab_order_cur = 0, slab_order_next = 0;
	size_t objsize = 0;
	struct small_mempool *cur_order_pool = &alloc->small_mempool_cache[0];
	alloc->small_mempool_groups_size = 0;

	for (alloc->small_mempool_cache_size = 0;
	     objsize < alloc->objsize_max &&
	     alloc->small_mempool_cache_size < SMALL_MEMPOOL_MAX;
	     alloc->small_mempool_cache_size++) {
		size_t prevsize = objsize;
		uint32_t mempool_cache_size = alloc->small_mempool_cache_size;
		objsize = small_class_calc_size_by_offset(&alloc->small_class,
							  mempool_cache_size);
		if (objsize > alloc->objsize_max)
			objsize = alloc->objsize_max;
		struct small_mempool *pool =
			&alloc->small_mempool_cache[mempool_cache_size];
		mempool_create(&pool->pool, alloc->cache, objsize);
		pool->pool.small_mempool = pool;
		pool->objsize_min = prevsize + 1;

		slab_order_cur = (slab_order_cur == 0 ?
				  pool->pool.slab_order : slab_order_cur);
		slab_order_next = pool->pool.slab_order;
		/*
		 * In the case when the size of slab changes, create one or
		 * more mempool groups. The count of groups depends on the
		 * mempools count with same slab size. There can be no more
		 * than 32 pools in one group.
		 */
		if (slab_order_next != slab_order_cur) {
			slab_order_cur = slab_order_next;
			small_mempool_create_groups(alloc, cur_order_pool, pool - 1);
			cur_order_pool = pool;
		}
		/*
		 * Maximum object size for mempool allocation == alloc->objsize_max.
		 * If we have reached this size, there will be no more pools - loop
		 * will be breaked at the next iteration. So wee need to create last
		 * group of pools.
		 */
		if (objsize == alloc->objsize_max)
			small_mempool_create_groups(alloc, cur_order_pool, pool);
	}
	alloc->objsize_max = objsize;
}

/** Initialize the small allocator. */
void
small_alloc_create(struct small_alloc *alloc, struct slab_cache *cache,
		   uint32_t objsize_min, unsigned granularity,
		   float alloc_factor, float *actual_alloc_factor)
{
	alloc->cache = cache;
	/* Align sizes. */
	objsize_min = small_align(objsize_min, granularity);
	/* Make sure at least 4 largest objects can fit in a slab. */
	alloc->objsize_max =
		mempool_objsize_max(slab_order_size(cache, cache->order_max));
	alloc->objsize_max = small_align(alloc->objsize_max, granularity);

	assert((granularity & (granularity - 1)) == 0);
	assert(alloc_factor > 1. && alloc_factor <= 2.);

	alloc->factor = alloc_factor;
	/*
	 * Second parameter granularity, determines alignment.
	 */
	small_class_create(&alloc->small_class, granularity,
			   alloc->factor, objsize_min, actual_alloc_factor);
	small_mempool_create(alloc);

	lifo_init(&alloc->delayed);
	lifo_init(&alloc->delayed_large);
	alloc->free_mode = SMALL_FREE;
}

void
small_alloc_setopt(struct small_alloc *alloc, enum small_opt opt, bool val)
{
	switch (opt) {
	case SMALL_DELAYED_FREE_MODE:
		alloc->free_mode = val ? SMALL_DELAYED_FREE :
			SMALL_COLLECT_GARBAGE;
		break;
	default:
		assert(false);
		break;
	}
}

static inline void
small_collect_garbage(struct small_alloc *alloc)
{
	if (alloc->free_mode != SMALL_COLLECT_GARBAGE)
		return;

	const int BATCH = 100;
	if (!lifo_is_empty(&alloc->delayed_large)) {
		/* Free large allocations */
		for (int i = 0; i < BATCH; i++) {
			void *item = lifo_pop(&alloc->delayed_large);
			if (item == NULL)
				break;
			struct slab *slab = slab_from_data(item);
			slab_put_large(alloc->cache, slab);
		}
	} else if (!lifo_is_empty(&alloc->delayed)) {
		/* Free regular allocations */
		struct mempool *pool = lifo_peek(&alloc->delayed);
		for (int i = 0; i < BATCH; i++) {
			void *item = lifo_pop(&pool->delayed);
			if (item == NULL) {
				(void) lifo_pop(&alloc->delayed);
				pool = lifo_peek(&alloc->delayed);
				if (pool == NULL)
					break;
				continue;
			}

			/*
			 * Find mempool from which the memory was actually
			 * allocated and recalculate waste if nedeed.
			 */
			struct mslab *slab = (struct mslab *)
				slab_from_ptr(item, pool->slab_ptr_mask);
			if (pool->small_mempool !=
			    slab->mempool->small_mempool) {
				assert(slab->mempool ==
				       &slab->mempool->small_mempool->pool);
				--pool->small_mempool->group->non_optimal_alloc;
			}
			mempool_free_slab(slab->mempool, slab, item);
		}
	} else {
		/* Finish garbage collection and switch to regular mode */
		alloc->free_mode = SMALL_FREE;
	}
}


/**
 * Allocate a small object.
 *
 * Find a mempool instance of the right size, using
 * small_class, and allocate the object on the pool.
 *
 * @retval ptr success
 * @retval NULL out of memory
 */
void *
smalloc(struct small_alloc *alloc, size_t size)
{
	small_collect_garbage(alloc);

	struct small_mempool *small_mempool = small_mempool_search(alloc, size);
	if (small_mempool == NULL) {
		/* Object is too large, fallback to slab_cache */
		struct slab *slab = slab_get_large(alloc->cache, size);
		if (slab == NULL)
			return NULL;
		return slab_data(slab);
	}
	if (small_mempool->group->non_optimal_alloc <
	    small_mempool->group->non_optimal_alloc_max) {
		++small_mempool->group->non_optimal_alloc;
		small_mempool = small_mempool->group->last_in_group;
	}

	return mempool_alloc(&small_mempool->pool);
}

static inline struct mempool *
mempool_find(struct small_alloc *alloc, size_t size)
{
	struct small_mempool *small_mempool = small_mempool_search(alloc, size);
	if (small_mempool == NULL)
		return NULL; /* Allocated by slab_cache. */
	assert(size >= small_mempool->objsize_min);
	struct mempool *pool = &small_mempool->pool;
	assert(size <= pool->objsize);
	return pool;
}

/** Free memory chunk allocated by the small allocator. */
/**
 * Free a small object.
 *
 * This boils down to finding the object's mempool and delegating
 * to mempool_free().
 */
void
smfree(struct small_alloc *alloc, void *ptr, size_t size)
{
	struct small_mempool *pool = small_mempool_search(alloc, size);
	if (pool == NULL) {
		/* Large allocation by slab_cache */
		struct slab *slab = slab_from_data(ptr);
		slab_put_large(alloc->cache, slab);
		return;
	}

	struct mslab *slab = (struct mslab *)
		slab_from_ptr(ptr, pool->pool.slab_ptr_mask);
	/*
	 * In case this ptr was allocated from other small mempool
	 * reducing waste for current pool (as you remember, waste
	 * in our case is memory loss due to allocation from large pools).
	 */
	if (pool != slab->mempool->small_mempool) {
		assert(slab->mempool == &slab->mempool->small_mempool->pool);
		--pool->group->non_optimal_alloc;
	}

	/* Regular allocation in mempools */
	mempool_free_slab(slab->mempool, slab, ptr);
}

/**
 * Free memory chunk allocated by the small allocator
 * if not in snapshot mode, otherwise put to the delayed
 * free list.
 */
void
smfree_delayed(struct small_alloc *alloc, void *ptr, size_t size)
{
	if (alloc->free_mode == SMALL_DELAYED_FREE && ptr) {
		struct mempool *pool = mempool_find(alloc, size);
		if (pool == NULL) {
			/* Large-object allocation by slab_cache. */
			lifo_push(&alloc->delayed_large, ptr);
			return;
		}
		/* Regular allocation in mempools */
		if (lifo_is_empty(&pool->delayed))
			lifo_push(&alloc->delayed, &pool->link);
		lifo_push(&pool->delayed, ptr);
	} else {
		smfree(alloc, ptr, size);
	}
}

/** Simplify iteration over small allocator mempools. */
struct mempool_iterator
{
	struct small_alloc *alloc;
	uint32_t small_iterator;
};

void
mempool_iterator_create(struct mempool_iterator *it,
			struct small_alloc *alloc)
{
	it->alloc = alloc;
	it->small_iterator = 0;
}

struct mempool *
mempool_iterator_next(struct mempool_iterator *it)
{
	struct small_mempool *small_mempool = NULL;
	if (it->small_iterator < it->alloc->small_mempool_cache_size)
		small_mempool =
			&it->alloc->small_mempool_cache[(it->small_iterator)++];
	if (small_mempool)
		return &(small_mempool->pool);

	return NULL;
}

/** Destroy all pools. */
void
small_alloc_destroy(struct small_alloc *alloc)
{
	struct mempool_iterator it;
	mempool_iterator_create(&it, alloc);
	struct mempool *pool;
	while ((pool = mempool_iterator_next(&it))) {
		mempool_destroy(pool);
	}
	lifo_init(&alloc->delayed);

	/* Free large allocations */
	void *item;
	while ((item = lifo_pop(&alloc->delayed_large))) {
		struct slab *slab = slab_from_data(item);
		slab_put_large(alloc->cache, slab);
	}
}

/** Calculate allocation statistics. */
void
small_stats(struct small_alloc *alloc,
	    struct small_stats *totals,
	    mempool_stats_cb cb, void *cb_ctx)
{
	memset(totals, 0, sizeof(*totals));

	struct mempool_iterator it;
	mempool_iterator_create(&it, alloc);
	struct mempool *pool;

	while ((pool = mempool_iterator_next(&it))) {
		struct mempool_stats stats;
		mempool_stats(pool, &stats);
		totals->used += stats.totals.used;
		totals->total += stats.totals.total;
		if (cb(&stats, cb_ctx))
			break;
	}
}
