/*
 * Copyright 2010-2021, Tarantool AUTHORS, please see AUTHORS file.
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
#include <small/small.h>
#include <small/quota.h>

#include <array>
#include <vector>
#include <utility>
#include <cmath>

#include <benchmark/benchmark.h>

enum {
	/** Minimum object size for allocation */
	OBJSIZE_MIN = 3 * sizeof(int),
	/**
	 * The number of memory allocation/deallocation operations
	 * for a performance test with objects of the same size.
	 */
	OBJECTS_SAME_SIZE_MAX = 1000000,
	/**
	 * The number of memory allocation/deallocation operations
	 * for a performance test with objects of the random size.
	 */
	OBJECTS_RANDOM_SIZE_MAX = 5000,
	/**
	 * The number of memory allocation/deallocation operations for
	 * a performance test with objects whose size grows exponentially.
	 */
	OBJECTS_EXP_SIZE_MAX = 10000,
};

enum {
	SLAB_SIZE_MIN = 4 * 1024 * 1024,
	SLAB_SIZE_MAX = 16 * 1024 * 1024,
};

const static std::array<double, 6> slab_alloc_factor = {
	1.01, 1.03, 1.05, 1.1, 1.3, 1.5
};

static struct slab_arena arena;
static struct slab_cache cache;
static struct small_alloc alloc;
static struct quota quota;

/**
 * Calculates the exponential growth factor of the size of objects.
 * The growth factor should be such that at each iteration of the test,
 * the size of the new object is larger than the size of the previous object
 * in a pow_factor times. At the same time, on the last iteration, the object
 * size must be equal to size_max.
 * @param[in] size_max - maximum object size.
 * @param[in] iterations - count of test iterations, each iteration allocates
 *                         one object.
 * @param[in] size_min - minimum object size.
 * @return exponential growth factor of the size of objects.
 */
static inline double
calculate_pow_factor(unsigned size_max, unsigned iterations, unsigned size_min)
{
	return exp(log((double)size_max / size_min) / iterations);
}

/**
 * Frees the memory of all previously allocated objects, saved in vector
 * @param[in] v - vector of all previously allocated objects and their sizes.
 */
static inline void
free_objects(std::vector<std::pair<void *, unsigned>>& v)
{
	for (unsigned int i = 0; i < v.size(); i++)
		smfree(&alloc, v[i].first, v[i].second);
}

/**
 * Allocates memory for an object and stores it in a vector with it's size.
 * @param[in] v - vector of all previously allocated objects and their sizes.
 * @param[in] size - memory size of the object.
 * @return true if success, otherwise return false.
 */
static inline bool
alloc_object(std::vector<std::pair<void *, unsigned>>& v, unsigned size)
{
	void *p = smalloc(&alloc, size);
	if (p == NULL)
		return false;

	try {
		v.push_back({p, size});
	} catch(std::bad_alloc& e) {
		smfree(&alloc, p, size);
		return false;
	}
	return true;
}

static int
small_is_unused_cb(const struct mempool_stats *stats, void *arg)
{
	unsigned long *slab_total = (unsigned long *)arg;
	*slab_total += stats->slabsize * stats->slabcount;
	return 0;
}

/**
 * Checks that all memory in the allocator has been released.
 * @return true if all memory released, otherwise return false
 */
static bool
small_is_unused(void)
{
	struct small_stats totals;
	unsigned long slab_total = 0;
	small_stats(&alloc, &totals, small_is_unused_cb, &slab_total);
	if (totals.used > 0)
		return false;
	if (slab_cache_used(&cache) > slab_total)
		return false;
	return true;
}

/**
 * Initializes allocator in each of the tests.
 * @param[in] slab_size - arena slab_size
 * @param[in] alloc_factor - allocation factor for small_alloc
 */
static void
small_alloc_test_start(unsigned slab_size, float alloc_factor)
{
	float actual_alloc_factor;
	quota_init(&quota, UINT_MAX);
	slab_arena_create(&arena, &quota, 0, slab_size, MAP_PRIVATE);
	slab_cache_create(&cache, &arena);
	small_alloc_create(&alloc, &cache, OBJSIZE_MIN, alloc_factor,
			   &actual_alloc_factor);
}

/**
 * Destroys  allocator in each of the tests.
 */
static void
small_alloc_test_finish(void)
{
	small_alloc_destroy(&alloc);
	slab_cache_destroy(&cache);
	slab_arena_destroy(&arena);
}

/**
 * Tests the performance of allocations of objects of the same size.
 */
static void
small_alloc_same_size(benchmark::State& state)
{
	unsigned slab_size = state.range(0);
	float alloc_factor = slab_alloc_factor[state.range(1)];
	const unsigned alloc_size = 1024;
	std::vector<std::pair<void *, unsigned>> v;
	small_alloc_test_start(slab_size, alloc_factor);
	state.counters["alloc_factor"] = alloc_factor;

	for (auto _ : state) {
		if (! alloc_object(v, alloc_size)) {
			state.SkipWithError("Failed to allocate memory");
			goto finish;
		}
	}

finish:
	free_objects(v);
	if (! small_is_unused())
		state.SkipWithError("Not all memory was released");
	small_alloc_test_finish();
}

/**
 * Tests the performance of memory releases of objects of the same size.
 */
static void
small_free_same_size(benchmark::State& state)
{
	unsigned slab_size = state.range(0);
	float alloc_factor = slab_alloc_factor[state.range(1)];
	const unsigned alloc_size = 1024;
	small_alloc_test_start(slab_size, alloc_factor);
	std::vector<std::pair<void *, unsigned>> v;
	unsigned int cnt = 0;
	state.counters["alloc_factor"] = alloc_factor;

	for (unsigned int i = 0; i < OBJECTS_SAME_SIZE_MAX; i++) {
		if (! alloc_object(v, alloc_size)) {
			state.SkipWithError("Failed to allocate memory");
			goto finish;
		}
	}

	for (auto _ : state) {
		if (cnt >= v.size()) {
			state.SkipWithError("Incorrect iteration count");
			break;
		}
		smfree(&alloc, v[cnt].first, v[cnt].second);
		cnt++;
	}

	if (cnt != v.size())
		state.SkipWithError("Incorrect iteration count");
	v.clear();
finish:
	free_objects(v);
	if (! small_is_unused())
		state.SkipWithError("Not all memory was released");
	small_alloc_test_finish();
}

/**
 * Tests the performance of allocations of objects of the random size.
 */
static void
small_alloc_random_size(benchmark::State& state)
{
	unsigned slab_size = state.range(0);
	float alloc_factor = slab_alloc_factor[state.range(1)];
	std::vector<std::pair<void *, unsigned>> v;
	small_alloc_test_start(slab_size, alloc_factor);
	const unsigned size_min = OBJSIZE_MIN;
	const unsigned size_max = (int)alloc.objsize_max - 1;
	state.counters["alloc_factor"] = alloc_factor;

	for (auto _ : state) {
		unsigned size = size_min + (rand() % (size_max - size_min));
		if (! alloc_object(v, size)) {
			state.SkipWithError("Failed to allocate memory");
			goto finish;
		}
	}

finish:
	free_objects(v);
	if (! small_is_unused())
		state.SkipWithError("Not all memory was released");
	small_alloc_test_finish();
}

/**
 * Tests the performance of memory releases of objects of the random size.
 */
static void
small_free_random_size(benchmark::State& state)
{
	unsigned slab_size = state.range(0);
	float alloc_factor = slab_alloc_factor[state.range(1)];
	small_alloc_test_start(slab_size, alloc_factor);
	std::vector<std::pair<void *, unsigned>> v;
	unsigned int cnt = 0;
	const unsigned size_min = OBJSIZE_MIN;
	const unsigned size_max = (int)alloc.objsize_max - 1;
	state.counters["alloc_factor"] = alloc_factor;

	for (unsigned int i = 0; i < OBJECTS_RANDOM_SIZE_MAX; i++) {
		unsigned size = size_min + (rand() % (size_max - size_min));
		if (! alloc_object(v, size)) {
			state.SkipWithError("Failed to allocate memory");
			goto finish;
		}
	}

	for (auto _ : state) {
		if (cnt >= v.size()) {
			state.SkipWithError("Incorrect iteration count");
			break;
		}
		smfree(&alloc, v[cnt].first, v[cnt].second);
		cnt++;
	}

	if (cnt != v.size())
		state.SkipWithError("Incorrect iteration count");
	v.clear();
finish:
	free_objects(v);
	if (! small_is_unused())
		state.SkipWithError("Not all memory was released");
	small_alloc_test_finish();
}

/**
 * Tests the performance of allocations of objects
 * whose size grows exponentially.
 */
static void
small_alloc_exp_size(benchmark::State& state)
{
	unsigned slab_size = state.range(0);
	float alloc_factor = slab_alloc_factor[state.range(1)];
	std::vector<std::pair<void *, unsigned>> v;
	small_alloc_test_start(slab_size, alloc_factor);
	const unsigned size_min = OBJSIZE_MIN;
	const unsigned size_max = (int)alloc.objsize_max - 1;
	double pow_factor =
		calculate_pow_factor(size_max, OBJECTS_EXP_SIZE_MAX, size_min);
	unsigned cnt = 0;
	state.counters["alloc_factor"] = alloc_factor;

	for (auto _ : state) {
		unsigned size = floor(size_min * pow(pow_factor, cnt++));
		if (size > size_max) {
			state.SkipWithError("Invalid object size");
			goto finish;
		}
		if (! alloc_object(v, size)) {
			state.SkipWithError("Failed to allocate memory");
			goto finish;
		}
	}

finish:
	free_objects(v);
	if (! small_is_unused())
		state.SkipWithError("Not all memory was released");
	small_alloc_test_finish();
}

/**
 * Tests the performance of memory releases of objects
 * whose size grows exponentially.
 */
static void
small_free_exp_size(benchmark::State& state)
{
	unsigned slab_size = state.range(0);
	float alloc_factor = slab_alloc_factor[state.range(1)];
	std::vector<std::pair<void *, unsigned>> v;
	small_alloc_test_start(slab_size, alloc_factor);
	const unsigned size_min = OBJSIZE_MIN;
	const unsigned size_max = (int)alloc.objsize_max - 1;
	double pow_factor =
		calculate_pow_factor(size_max, OBJECTS_EXP_SIZE_MAX, size_min);
	unsigned cnt = 0;
	state.counters["alloc_factor"] = alloc_factor;

	for (unsigned int i = 0; i < OBJECTS_EXP_SIZE_MAX; i++) {
		unsigned size = floor(size_min * pow(pow_factor, cnt++));
		if (! alloc_object(v, size)) {
			state.SkipWithError("Failed to allocate memory");
			goto finish;
		}
	}

	cnt = 0;
	for (auto _ : state) {
		if (cnt >= v.size()) {
			state.SkipWithError("Incorrect iteration count");
			break;
		}
		smfree(&alloc, v[cnt].first, v[cnt].second);
		cnt++;
	}

	if (cnt != v.size())
		state.SkipWithError("Incorrect iteration count");
	v.clear();
finish:
	free_objects(v);
	if (! small_is_unused())
		state.SkipWithError("Not all memory was released");
	small_alloc_test_finish();
}

static void CustomArguments(benchmark::internal::Benchmark* b)
{
	for (unsigned i = SLAB_SIZE_MIN; i <= SLAB_SIZE_MAX; i *= 2)
		for (unsigned j = 0; j < slab_alloc_factor.size(); j ++)
			b->Args({i, j});
}
BENCHMARK(small_alloc_same_size)
	->Apply(CustomArguments)->Iterations(OBJECTS_SAME_SIZE_MAX);
BENCHMARK(small_free_same_size)
	->Apply(CustomArguments)->Iterations(OBJECTS_SAME_SIZE_MAX);
BENCHMARK(small_alloc_random_size)
	->Apply(CustomArguments)->Iterations(OBJECTS_RANDOM_SIZE_MAX);
BENCHMARK(small_free_random_size)
	->Apply(CustomArguments)->Iterations(OBJECTS_RANDOM_SIZE_MAX);
BENCHMARK(small_alloc_exp_size)
	->Apply(CustomArguments)->Iterations(OBJECTS_EXP_SIZE_MAX);
BENCHMARK(small_free_exp_size)
	->Apply(CustomArguments)->Iterations(OBJECTS_EXP_SIZE_MAX);

BENCHMARK_MAIN();
