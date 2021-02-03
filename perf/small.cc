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
#include <string>
#include <utility>
#include <cmath>
#include <iostream>

#include <benchmark/benchmark.h>

enum {
	/** Minimum object size for allocation */
	OBJSIZE_MIN = 3 * sizeof(int),
	/** Minimal slab size */
	SLAB_SIZE_MIN = 4194304,
	/** Maximal slab size */
	SLAB_SIZE_MAX = 16777216,
	/** Maximal label name */
	LABEL_NAME_MAX = 10,
};

struct becnhmark_args {
	/** Minimal size of objects in benchmark */
	unsigned size_min;
	/** Maximal size of objects in benchmark */
	unsigned size_max;
	/** Prealloc objects count */
	unsigned prealloc;
};

static struct slab_arena arena;
static struct slab_cache cache;
static struct small_alloc alloc;
static struct quota quota;
static std::array<struct becnhmark_args, 2> objsize_arr = { {
	{ 20, 100, 1000000 },
	{ 1000, 10000, 250000 },
} };
static std::array<float, 2> alloc_factor_arr = { 1.05, 1.5 };

static void
print_description_header(void)
{
	std::cout << std::endl << std::endl;
	std::cout << "******************************************************"
		"********************************" << std::endl;
	std::cout << "This benchmark checks performance of memory allocation "
		"and deallocation operations   *" << std::endl <<
		"for typical workload.";
	std::cout << "First of all test allocates 250000 objects with size "
		"ranging    *" << std::endl << "from 20 to 100 bytes, or "
		"1000000 objects with size ranging from 1000 to 10000 bytes  *"
		<< std::endl << "and push it in the vector.    "
		"                                                  "
		"     *" << std ::endl;
	std::cout << "Then, in a loop, test allocates memory for an object of"
		" the appropriate size, push   *" << std::endl <<
		"it in the vector, and frees up memory for a random object"
		" in the vector. Test checks *" << std::endl <<
		"and print time of one pair of memory allocation and" <<
		" deallocation operations and also *" << std::endl <<
		"count of operations.                        " <<
		"                                         *" << std::endl;
	std::cout << "Test also checks performance for different alloc_factor"
		<< " and slab_size.               *" << std::endl;
	std::cout << "******************************************************"
		"********************************" << std::endl;
	std::cout << std::endl << std::endl;
}

/**
 * Frees memory of one random object in vector
 */
static inline void
free_object(std::vector<std::pair<void *, unsigned>>& v,
	    benchmark::State &state)
{
	unsigned int i = 0;
	state.PauseTiming();
	do {
		i = rand() % v.size();
	} while (v[i].first == NULL);
	state.ResumeTiming();

	smfree(&alloc, v[i].first, v[i].second);
	/*
	 * Erase is too long for vector, so we only mark
	 * this element as deleted.
	 */
	benchmark::DoNotOptimize(v[i].first = NULL);
}

/**
 * Frees memory of all previously allocated objects, saved in vector
 * @param[in] v - vector of all previously allocated objects and their sizes.
 */
static inline void
free_objects(std::vector<std::pair<void *, unsigned>>& v)
{
	for (unsigned int i = 0; i < v.size(); i++) {
		if (v[i].first != NULL)
			smfree(&alloc, v[i].first, v[i].second);
	}
	v.clear();
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
	small_alloc_create(&alloc, &cache, OBJSIZE_MIN, sizeof(intptr_t),
			   alloc_factor, &actual_alloc_factor);
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

static void
small_workload_benchmark(benchmark::State& state)
{
	unsigned slab_size = state.range(0);
	unsigned size_min = state.range(1);
	unsigned size_max = state.range(2);
	unsigned prealloc_objcount = state.range(3);
	unsigned alloc_factor_idx = state.range(4);
	float alloc_factor = alloc_factor_arr[alloc_factor_idx];
	char label[LABEL_NAME_MAX];
	std::vector<std::pair<void *, unsigned>> v;
	small_alloc_test_start(slab_size, alloc_factor);
	std::snprintf(label, LABEL_NAME_MAX, "%4.2f", alloc_factor);
	state.SetLabel(std::string("alloc_factor: ") +
		       std::string(label));

	for (unsigned int i = 0; i < prealloc_objcount; i++) {
		unsigned size = size_min + (rand() % (size_max - size_min));
		if (! alloc_object(v, size)) {
			state.SkipWithError("Failed to allocate memory");
			goto finish;
		}
	}

	for (auto _ : state) {
		unsigned size = size_min + (rand() % (size_max - size_min));
		if (! alloc_object(v, size)) {
			state.SkipWithError("Failed to allocate memory");
			goto finish;
		}
		free_object(v, state);
	}

finish:
	free_objects(v);
	if (! small_is_unused())
		state.SkipWithError("Not all memory was released");
	small_alloc_test_finish();
}

static void
generate_benchmark_args(benchmark::internal::Benchmark* b)
{
	for (unsigned size = SLAB_SIZE_MIN; size <= SLAB_SIZE_MAX; size *= 2) {
		for (unsigned j = 0; j < objsize_arr.size(); j++) {
			for (unsigned k = 0; k < alloc_factor_arr.size(); k++) {
				b->Args( {
					    size, objsize_arr[j].size_min,
					    objsize_arr[j].size_max,
					    objsize_arr[j].prealloc,
					    k
					  }
					);
			}
		}
	}
}

BENCHMARK(small_workload_benchmark)
	->Apply(generate_benchmark_args)
	->ArgNames({"slab_size", "size_min", "size_max", "prealloc"});

int main(int argc, char** argv)
{
	::benchmark::Initialize(&argc, argv);
	if (::benchmark::ReportUnrecognizedArguments(argc, argv))
		return 1;
	print_description_header();
	::benchmark::RunSpecifiedBenchmarks();
}
