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
 * Updates info in pool group for all pools with
 * object size less than or equal this pool object size.
 * First of all mempools allocates memory from the greatest
 * mempool in group. Then when waste for some mempool in
 * group becames greater or equal then waste_max, we mark this
 * mempool as available for allocation for all mempools in group,
 * with object size less than or equal this pool object size,
 * using this function.
 * @param[in] factor_pool - factor pool, whose waste has become greater or
 *                          equal then waste_max. Now for all objects whose
 *                          size are in the range from factor_pool->objsize_min
 *                          to factor_pool->pool.objsize the memory will be
 *                          allocated from this factor_pool. Also all pools in
 *                          group with smaller object size and waste less then
 *                          waste_max will allocate memory from this pool.
 */
static inline void
factor_pool_update_group(struct factor_pool *factor_pool)
{
	factor_pool->used_pool = factor_pool;
	factor_pool->used_pool_mask |= (UINT32_C(1) << factor_pool->idx);
	/*
	 * Calculate first pool in group
	 */
	struct factor_pool *first = factor_pool - factor_pool->idx;
	/*
	 * All pools in group have same slab_order and slab size
	 */
	assert(first->pool.slab_order == factor_pool->pool.slab_order);
	/*
	 * We update info for all pools in group with object size <= this pool
	 * object size (we don't need to update other pools info, because
	 * larger pools in group never use this pool for allocation).
	 */
	for (struct factor_pool *pool = first; pool != factor_pool; pool++) {
		if (pool != pool->used_pool) {
			/*
			 * First we update used_pool_mask marking the new pool
			 * as available for allocation
			 */
			pool->used_pool_mask |=
				(UINT32_C(1) << factor_pool->idx);
			/*
			 * Recalculate pool index for allocation.
			 * We select the pool with the lowest index,
			 * thus reducing allocation waste.
			 */
			int used_idx =
				__builtin_ffs(pool->used_pool_mask) -
				1 - pool->idx;
			assert(pool->used_pool == NULL ||
			       (pool + used_idx <= pool->used_pool));
			pool->used_pool = pool + used_idx;
		}
	}
}

/**
 * Calculates last pool in group.
 * In a group of pools with the same pool size,
 * there can be no more than 32 pools
 * @param[in] first - first pool in new group.
 * @param[in] last - last pool with same slab_size.
 * @return last factor pool in group.
 */
static inline struct factor_pool*
calculate_last_pool_in_group(struct factor_pool *first,
			     struct factor_pool *last)
{
	uint32_t c = 0;
	struct factor_pool *last_in_group = first;
	while (last_in_group < last && (c++) < POOL_PER_GROUP_MAX - 1)
		last_in_group++;
	return last_in_group;
}

/**
 * Creates new factor pool group. First assigns all the pools
 * in the group their indexes, then mark last pool in group as
 * available for allocation. From now all mempools in group will
 * allocate memory from it, as long as their waste < waste_max.
 * @param[in] first -first pool in group.
 * @param[in] last - last pool in group.
 */
static inline void
factor_pool_create_group(struct factor_pool *first, struct factor_pool *last)
{
	unsigned idx = 0;
	while (first <= last) {
		first->idx = idx++;
		assert(first->idx < 32);
		first++;
	}
	factor_pool_update_group(last);
}

/**
 * Creates one or more groups of pools from pools with the same slab size.
 * (One group for 32 or less pools).
 * @param[in] first - First pool with same slab size.
 * @param[in] last - Last pool with same slab size.
 */
static inline void
factor_pool_create_groups(struct factor_pool *first, struct factor_pool *last)
{
	struct factor_pool *first_in_group = first;
	while (first_in_group <= last) {
		struct factor_pool *last_in_group =
			calculate_last_pool_in_group(first_in_group, last);
		factor_pool_create_group(first_in_group, last_in_group);
		first_in_group = last_in_group + 1;
	}
}

static inline struct factor_pool *
factor_pool_search(struct small_alloc *alloc, size_t size)
{
	if (size > alloc->objsize_max)
		return NULL;
	unsigned cls =
		small_class_calc_offset_by_size(&alloc->small_class, size);
	struct factor_pool *pool = &alloc->factor_pool_cache[cls];
	return pool;
}

static inline void
factor_pool_create(struct small_alloc *alloc)
{
	uint32_t slab_order_cur = 0, slab_order_next = 0;
	size_t objsize = 0;
	struct factor_pool *cur_order_pool = &alloc->factor_pool_cache[0];

	for (alloc->factor_pool_cache_size = 0;
	     objsize < alloc->objsize_max &&
	     alloc->factor_pool_cache_size < FACTOR_POOL_MAX;
	     alloc->factor_pool_cache_size++) {
		size_t prevsize = objsize;
		objsize = small_class_calc_size_by_offset(&alloc->small_class,
			alloc->factor_pool_cache_size);
		if (objsize > alloc->objsize_max)
			objsize = alloc->objsize_max;
		struct factor_pool *pool =
			&alloc->factor_pool_cache[alloc->factor_pool_cache_size];
		mempool_create(&pool->pool, alloc->cache, objsize);
		pool->pool.factor_pool = pool;
		pool->objsize_min = prevsize + 1;
		pool->used_pool_mask = 0;
		pool->idx = 0;
		pool->used_pool = NULL;
		pool->waste = 0;
		pool->waste_max = slab_order_size(pool->pool.cache,
						  pool->pool.slab_order) / 4;

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
			factor_pool_create_groups(cur_order_pool, pool - 1);
			cur_order_pool = pool;
		}

		if (objsize == alloc->objsize_max)
			factor_pool_create_groups(cur_order_pool, pool);
	}
	alloc->objsize_max = objsize;
}

/** Initialize the small allocator. */
void
small_alloc_create(struct small_alloc *alloc, struct slab_cache *cache,
		   uint32_t objsize_min, float alloc_factor,
		   float *actual_alloc_factor)
{
	alloc->cache = cache;
	/* Align sizes. */
	objsize_min = small_align(objsize_min, sizeof(intptr_t));
	/* Make sure at least 4 largest objects can fit in a slab. */
	alloc->objsize_max =
		mempool_objsize_max(slab_order_size(cache, cache->order_max));

	assert(alloc_factor > 1. && alloc_factor <= 2.);

	alloc->factor = alloc_factor;
	/*
	 * Second parameter (uintptr_t) - granularity,
	 * determines alignment.
	 */
	small_class_create(&alloc->small_class, sizeof(intptr_t),
			   alloc->factor, objsize_min, actual_alloc_factor);
	factor_pool_create(alloc);

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
			if (pool->factor_pool != slab->mempool->factor_pool) {
				assert(slab->mempool ==
				       &slab->mempool->factor_pool->pool);
				pool->factor_pool->waste -=
					(slab->mempool->objsize -
					 pool->objsize);
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
 * Find or create a mempool instance of the right size,
 * and allocate the object on the pool.
 *
 * If object is small enough to fit a stepped pool,
 * finding the right pool for it is just a matter of bit
 * shifts. Otherwise, look up a pool in the red-black
 * factored pool tree.
 *
 * @retval ptr success
 * @retval NULL out of memory
 */
void *
smalloc(struct small_alloc *alloc, size_t size)
{
	small_collect_garbage(alloc);

	struct factor_pool *upper_bound = factor_pool_search(alloc, size);
	if (upper_bound == NULL) {
		/* Object is too large, fallback to slab_cache */
		struct slab *slab = slab_get_large(alloc->cache, size);
		if (slab == NULL)
			return NULL;
		return slab_data(slab);
	}
	if (upper_bound->used_pool != upper_bound) {
		/*
		 * Waste for this allocation is the difference between
		 * the size of objects optimum mempool and used mempool
		 */
		upper_bound->waste +=
			(upper_bound->used_pool->pool.objsize -
			 upper_bound->pool.objsize);
		/*
		 * In case when waste for this mempool became greater then
		 * or equal to waste_max, we are updating the information
		 * for the mempool group that this mempool belongs to,
		 * that it can now be used for memory allocation.
		 */
		if (upper_bound->waste >= upper_bound->waste_max)
			factor_pool_update_group(upper_bound);
	}

	struct mempool *pool = &upper_bound->used_pool->pool;
	assert(size <= pool->objsize);
	return mempool_alloc(pool);
}

static inline struct mempool *
mempool_find(struct small_alloc *alloc, size_t size)
{
	struct factor_pool *upper_bound = factor_pool_search(alloc, size);
	if (upper_bound == NULL)
		return NULL; /* Allocated by slab_cache. */
	assert(size >= upper_bound->objsize_min);
	struct mempool *pool = &upper_bound->pool;
	assert(size <= pool->objsize);
	return pool;
}

/** Free memory chunk allocated by the small allocator. */
/**
 * Free a small object.
 *
 * This boils down to finding the object's mempool and delegating
 * to mempool_free().
 *
 * If the pool becomes completely empty, and it's a factored pool,
 * and the factored pool's cache is empty, put back the empty
 * factored pool into the factored pool cache.
 */
void
smfree(struct small_alloc *alloc, void *ptr, size_t size)
{
	struct factor_pool *pool = factor_pool_search(alloc, size);
	if (pool == NULL) {
		/* Large allocation by slab_cache */
		struct slab *slab = slab_from_data(ptr);
		slab_put_large(alloc->cache, slab);
		return;
	}

	struct mslab *slab = (struct mslab *)
		slab_from_ptr(ptr, pool->pool.slab_ptr_mask);
	/*
	 * In case this ptr was allocated from other factor pool
	 * reducing waste for current pool (as you remember, waste
	 * in our case is memory loss due to allocation from large pools).
	 */
	if (pool != slab->mempool->factor_pool) {
		assert(slab->mempool == &slab->mempool->factor_pool->pool);
		pool->waste -= (slab->mempool->objsize - pool->pool.objsize);
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
	uint32_t factor_iterator;
};

void
mempool_iterator_create(struct mempool_iterator *it,
			struct small_alloc *alloc)
{
	it->alloc = alloc;
	it->factor_iterator = 0;
}

struct mempool *
mempool_iterator_next(struct mempool_iterator *it)
{
	struct factor_pool *factor_pool = NULL;
	if (it->factor_iterator < it->alloc->factor_pool_cache_size)
		factor_pool =
			&it->alloc->factor_pool_cache[(it->factor_iterator)++];
	if (factor_pool)
		return &(factor_pool->pool);

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
