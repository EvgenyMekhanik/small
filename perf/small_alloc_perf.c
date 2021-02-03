#include <small/small.h>
#include <small/quota.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <math.h>
#include "../test/unit.h"

enum {
	OBJSIZE_MIN = 3 * sizeof(int),
	OBJECTS_MAX = 1000,
	OBJECTS_SAME_MAX = 10000,
};

unsigned int basic_iterations_count = 10000;
unsigned int large_iterations_count = 1000;
const size_t SLAB_SIZE_MIN = 4 * 1024 * 1024;
const size_t SLAB_SIZE_MAX = 16 * 1024 * 1024;
static const unsigned long long NANOSEC_PER_SEC  = 1000000000;
/**
 * True in the case where we don't need to test performance
 * of memory allocation for random size objects.
 */
static bool no_basic_random_test = false;
/**
 * True in the case where we don't need to test performance
 * of memory allocation for exponent grow size objects.
 */
static bool no_basic_exp_test = false;
/**
 * True in the case where we don't need to test performance
 * of memory allocation for same size objects.
 */
static bool no_basic_same_test = false;
/**
 * True in the case where we don't need to test performance
 * of memory allocation for large size objects.
 */
static bool no_large_test = false;
/**
 * True in the case where we don't need to
 * test delayed free mode.
 */
static bool no_delayed_free_mode = false;
#define SZR(arr) sizeof(arr) / sizeof(arr[0])

float slab_alloc_factor[] = {1.01, 1.03, 1.05, 1.1, 1.3, 1.5};

struct slab_arena arena;
struct slab_cache cache;
struct small_alloc alloc;
struct quota quota;
/** Streak type - allocating or freeing */
bool allocating = true;
/** Enable human output */
bool human = false;
/** Keep global to easily inspect the core. */
long seed;
char json_output[100000];
size_t length = sizeof(json_output);
size_t pos = 0;

static int *ptrs[OBJECTS_SAME_MAX];

/*
 * Fucntion calculates timediff in nanoseconds
 */
static inline
long long int timediff(const struct timespec *tm1, const struct timespec *tm2)
{
	return NANOSEC_PER_SEC * (tm2->tv_sec - tm1->tv_sec) +
		(tm2->tv_nsec - tm1->tv_nsec);
}

static inline void
free_checked(int *ptr)
{
	int pos = ptr[0];
	smfree_delayed(&alloc, ptrs[pos], ptrs[pos][1]);
	ptrs[pos] = NULL;
}

static float
calculate_pow_factor(const int size_max, const int pow_max, const int start)
{
	return exp(log((double)size_max / start) / pow_max);
}

static inline void *
alloc_checked(const int pos, const int size_min, const int size_max,
	      const int rnd, const double pow_factor)
{
	int size;
	if (ptrs[pos])
		free_checked(ptrs[pos]);

	if (!allocating)
		return NULL;

	if (rnd) {
		size = size_min + (rand() % (size_max - size_min));
	} else {
		size = floor(256 * pow(pow_factor, pos));
	}
	ptrs[pos] = smalloc(&alloc, size);
	/**
	 * In the previous version of test, we expected that in some cases
	 * function would return null, but now with the new memory allocation
	 * strategy, this should not happen.
	 */
	fail_unless(ptrs[pos] != NULL);
	/*
	 * Save position in the ptrs array and the size of object for the possibility
	 * of correct deletion of the object in the future.
	 */
	ptrs[pos][0] = pos;
	ptrs[pos][1] = size;
	return ptrs[pos];
}

static int
small_is_unused_cb(const struct mempool_stats *stats, void *arg)
{
	unsigned long *slab_total = arg;
	*slab_total += stats->slabsize * stats->slabcount;
	return 0;
}

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

/*
 * Function for testing the performance of
 * memory allocation and deallocation. Return
 * the total number of operations performed.
 *
 * @param[in] size_min - minimal object size for memory allocation
 * @param[in] size_max - maximal object size for memory allocation
 * @param[in] iterations_max - count of test iterations
 * @param[in] rnd - indicates how the object size is calculated
 *                  (randomly or not)
 * @param[in] cnt - count of allocated objects
 */
static unsigned long long
small_alloc_test(const int size_min, const int size_max,
		 const int iterations_max, const int rnd, const int cnt)
{
	unsigned long long count = 0;
	double pow_factor = calculate_pow_factor(size_max, cnt, 256);
	small_alloc_setopt(&alloc, SMALL_DELAYED_FREE_MODE, false);
	allocating = true;

	for (int i = 0; i <= iterations_max; i++) {
		if (!no_delayed_free_mode) {
			int mode = i % 3;
			switch (mode) {
			case 1:
				small_alloc_setopt(&alloc,
						   SMALL_DELAYED_FREE_MODE,
						   false);
				break;
			case 2:
				small_alloc_setopt(&alloc,
						   SMALL_DELAYED_FREE_MODE,
						   true);
				break;
			default:
				break;
			}
		}
		for (int j = 0; j < cnt; ++j) {
			alloc_checked(j, size_min, size_max, rnd, pow_factor);
			count++;
		}
		allocating = !allocating;
	}

	small_alloc_setopt(&alloc, SMALL_DELAYED_FREE_MODE, false);

	for (int pos = 0; pos < cnt; pos++) {
		if (ptrs[pos] != NULL) {
			count++;
			free_checked(ptrs[pos]);
		}
	}

	if (!no_delayed_free_mode) {
		/* Trigger garbage collection. */
		allocating = true;
		for (int i = 0; i < iterations_max; i++) {
			if (small_is_unused())
				break;
			void *p = alloc_checked(0, size_min, size_max,
						rnd, pow_factor);
			free_checked(p);
			count += 2;
		}
	}

	/*
	 * Checking that all objects are deleted
	 */
	fail_unless(small_is_unused());
	return count;
}

static void
print_json_test_header(const char *type)
{
	size_t x = snprintf(json_output + pos, length,
			    "        \"%s\": {\n", type);
	length -= x;
	pos += x;
	x = snprintf(json_output + pos, length,
		     "            \"alloc factor\": {\n");
	length -= x;
	pos += x;
	for (unsigned int i = 0; i < SZR(slab_alloc_factor); i++) {
		size_t x = snprintf(json_output + pos, length,
				    "                \"%.4f\"\n",
				    slab_alloc_factor[i]);
		length -= x;
		pos += x;
	}
	x = snprintf(json_output + pos, length, "            },\n");
	length -= x;
	pos += x;
	x = snprintf(json_output + pos, length,
		     "            \"mrps\": {\n");
	length -= x;
	pos += x;
}

static void
print_json_test_finish(const char * finish)
{
	size_t x = snprintf(json_output + pos, length, "            }\n");
	length -= x;
	pos += x;
	x = snprintf(json_output + pos, length, "        }%s\n", finish);
	length -= x;
	pos += x;
}

static void
print_json_test_result(const unsigned long long mrps)
{
	size_t x = snprintf(json_output + pos, length,
			    "                \"%llu\"\n", mrps);
	length -= x;
	pos += x;
}

static void
print_json_test_result_double(const double mrps)
{
	size_t x = snprintf(json_output + pos, length,
			    "                \"%.3f\"\n", mrps);
	length -= x;
	pos += x;
}

static void
small_alloc_basic(const unsigned int slab_size)
{
	struct timespec tm1, tm2;
	if (!no_basic_random_test) {
		/*
		 * Memory allocation/deallocation performance test for random size objects.
		 */
		if(human) {
			fprintf(stderr, "|              SMALL RANDOM "
				"ALLOCATION RESULT TABLE                  |\n");
			fprintf(stderr, "|___________________________________"
				"_________________________________|\n");
			fprintf(stderr, "|           alloc_factor          "
				" |   	           mrps              |\n");
			fprintf(stderr, "|__________________________________|"
				"_________________________________|\n");
		} else {
			print_json_test_header("random");
		}
		quota_init(&quota, UINT_MAX);
		slab_arena_create(&arena, &quota, 0, slab_size, MAP_PRIVATE);
		slab_cache_create(&cache, &arena);
		for (unsigned int i = 0; i < SZR(slab_alloc_factor); i++) {
			float actual_alloc_factor;
			small_alloc_create(&alloc, &cache,
					   OBJSIZE_MIN, slab_alloc_factor[i],
					   &actual_alloc_factor);
			int size_min = OBJSIZE_MIN;
			int size_max = (int)alloc.objsize_max - 1;
			fail_unless(clock_gettime(CLOCK_MONOTONIC, &tm1) == 0);
			unsigned long long count =
				small_alloc_test(size_min, size_max,
						 basic_iterations_count,
						 1, OBJECTS_MAX);
			fail_unless(clock_gettime(CLOCK_MONOTONIC, &tm2) == 0);
			if (human) {
				fprintf(stderr,
					"|              %.4f              |"
					"             %5llu               |\n",
					slab_alloc_factor[i], count /
					(1000000 * timediff(&tm1, &tm2) /
					 NANOSEC_PER_SEC)
				       );
			} else {
				print_json_test_result(count /
						       (1000000 *
							timediff(&tm1, &tm2) /
							NANOSEC_PER_SEC)
						      );
			}
			small_alloc_destroy(&alloc);
		}
		slab_cache_destroy(&cache);
		slab_arena_destroy(&arena);
	}


	if (!no_basic_exp_test) {
		/*
		 * Memory allocation/deallocation performance test for exponent grow size objects.
		 */
		quota_init(&quota, UINT_MAX);
		slab_arena_create(&arena, &quota, 0, slab_size, MAP_PRIVATE);
		slab_cache_create(&cache, &arena);
		if (human) {
			if (!no_basic_random_test) {
				fprintf(stderr,
					"|__________________________________|"
					"_________________________________|\n");
			}
			fprintf(stderr, "|             SMALL EXP GROW "
				"ALLOCATION RESULT TABLE                 |\n");
			fprintf(stderr, "|___________________________________"
				"_________________________________|\n");
			fprintf(stderr, "|           alloc_factor          "
				" |   	           mrps              |\n");
			fprintf(stderr, "|__________________________________|"
				"_________________________________|\n");
		} else {
			print_json_test_finish(",");
			print_json_test_header("exponent");
		}
		for (unsigned int i = 0; i < SZR(slab_alloc_factor); i++) {
			float actual_alloc_factor;
			small_alloc_create(&alloc, &cache,
					   OBJSIZE_MIN, slab_alloc_factor[i],
					   &actual_alloc_factor);
			int size_min = OBJSIZE_MIN;
			int size_max = (int)alloc.objsize_max - 1;
			fail_unless(clock_gettime(CLOCK_MONOTONIC, &tm1) == 0);
			unsigned long long count =
				small_alloc_test(size_min, size_max,
						 basic_iterations_count,
						 0, OBJECTS_MAX);
			fail_unless(clock_gettime(CLOCK_MONOTONIC, &tm2) == 0);
			if (human) {
				fprintf(stderr,
					"|              %.4f              |"
					"             %5llu               |\n",
					slab_alloc_factor[i], count /
					(1000000 * timediff(&tm1, &tm2) /
					 NANOSEC_PER_SEC)
				       );
			} else {
				print_json_test_result(count /
						       (1000000 *
							timediff(&tm1, &tm2) /
							NANOSEC_PER_SEC)
						      );
			}
			small_alloc_destroy(&alloc);
		}
	}

	if (!no_basic_same_test) {
		/*
		 * Memory allocation/deallocation performance test for same size objects.
		 */
		quota_init(&quota, UINT_MAX);
		slab_arena_create(&arena, &quota, 0, slab_size, MAP_PRIVATE);
		slab_cache_create(&cache, &arena);
		if (human) {
			if (!no_basic_random_test || !no_basic_exp_test) {
				fprintf(stderr,
					"|__________________________________|"
					"_________________________________|\n");
			}
			fprintf(stderr, "|             SMALL SAME SIZE "
				"ALLOCATION RESULT TABLE                |\n");
			fprintf(stderr, "|___________________________________"
				"_________________________________|\n");
			fprintf(stderr, "|           alloc_factor          "
				" |   	           mrps              |\n");
			fprintf(stderr, "|__________________________________|"
				"_________________________________|\n");
		} else {
			print_json_test_finish(",");
			print_json_test_header("same size");
		}
		for (unsigned int i = 0; i < SZR(slab_alloc_factor); i++) {
			float actual_alloc_factor;
			small_alloc_create(&alloc, &cache,
					   OBJSIZE_MIN, slab_alloc_factor[i],
					   &actual_alloc_factor);
			int size_min = OBJSIZE_MIN + 100;
			int size_max = size_min + 100;
			fail_unless(clock_gettime(CLOCK_MONOTONIC, &tm1) == 0);
			unsigned long long count =
				small_alloc_test(size_min, size_max,
						 basic_iterations_count,
						 1, OBJECTS_SAME_MAX);
			fail_unless(clock_gettime (CLOCK_MONOTONIC, &tm2) == 0);
			if (human) {
				fprintf(stderr,
					"|              %.4f              |"
					"             %5llu               |\n",
					slab_alloc_factor[i], count /
					(1000000 * timediff(&tm1, &tm2) /
					NANOSEC_PER_SEC)
				       );
			} else {
				print_json_test_result(count /
						       (1000000 *
							timediff(&tm1, &tm2) /
							NANOSEC_PER_SEC)
						      );
			}
			small_alloc_destroy(&alloc);
		}
		slab_cache_destroy(&cache);
		slab_arena_destroy(&arena);
	}

	if (!no_basic_random_test || !no_basic_exp_test ||
	    !no_basic_same_test) {
		if (human) {
			fprintf(stderr, "|___________________________________"
				"_________________________________|\n");
		} else {
			print_json_test_finish((no_large_test ? "" : ","));
		}
	}
}

static void
small_alloc_large()
{
	struct timespec tm1, tm2;
	size_t large_size_min = mempool_objsize_max(cache.arena->slab_size);
	size_t large_size_max = 2 * cache.arena->slab_size;
	if (human) {
		fprintf(stderr, "|              LARGE RANDOM "
			"ALLOCATION RESULT TABLE                  |\n");
		fprintf(stderr, "|___________________________________"
			"_________________________________|\n");
		fprintf(stderr, "|           alloc_factor          "
			" |   	           mrps              |\n");
		fprintf(stderr, "|__________________________________|"
			"_________________________________|\n");
	} else {
		print_json_test_header("large");
	}
	for (unsigned int i = 0; i < SZR(slab_alloc_factor); i++) {
		float actual_alloc_factor;
		small_alloc_create(&alloc, &cache, OBJSIZE_MIN,
				   slab_alloc_factor[i], &actual_alloc_factor);
		fail_unless(clock_gettime(CLOCK_MONOTONIC, &tm1) == 0);
		unsigned long long count =
			small_alloc_test(large_size_min, large_size_max,
					 large_iterations_count, 1, 25);
		fail_unless(clock_gettime(CLOCK_MONOTONIC, &tm2) == 0);
		if (human) {
			fprintf(stderr, "|              %.4f              |"
				"               %.3f             |\n",
				slab_alloc_factor[i], (double)count /
				(1000000 * timediff(&tm1, &tm2) /
				 NANOSEC_PER_SEC)
			       );
		} else {
			print_json_test_result_double((double)count /
						      (1000000 *
						       timediff(&tm1, &tm2) /
						       NANOSEC_PER_SEC)
						     );
		}
		small_alloc_destroy(&alloc);
	}
	if (human) {
		fprintf(stderr, "|___________________________________"
			"_________________________________|\n");
	} else {
		print_json_test_finish("");
	}
}

int main(int argc, char* argv[])
{
	size_t x;
	seed = time(0);
	srand(seed);

	if (argc == 2 && !strcmp(argv[1], "-h")) //human clear output
		human = true;

	for (int i = 1; i < argc; i++) {
		if (!strcmp(argv[i], "-h")) {
			human = true;
		} else if (!strcmp(argv[i], "--no-random-size-test")) {
			no_basic_random_test = true;
		} else if (!strcmp(argv[i], "--no-exp-grow-size-test")) {
			no_basic_exp_test = true;
		} else if (!strcmp(argv[i], "--no-same-size-test")) {
			no_basic_same_test = true;
		} else if (!strcmp(argv[i], "--no-large-size-test")) {
			no_large_test = true;
		} else if (!strcmp(argv[i], "--no-delayed-free-mode")) {
			no_delayed_free_mode = true;
		} else {
			fprintf(stderr, "Invalid option\n");
			return EXIT_FAILURE;
		}
	}


	if (!human) {
		x = snprintf(json_output + pos, length, "{\n");
		length -= x;
		pos += x;
	}
	for (unsigned int slab_size = SLAB_SIZE_MIN; slab_size <= SLAB_SIZE_MAX;
	     slab_size *= 2) {
		if(human) {
			fprintf(stderr, "_____________________________________"
				"_________________________________\n");
			fprintf(stderr, "|           PERFORMANCE TEST WITH SLABSIZE "
				"%8u BYTES            |\n", slab_size);
			fprintf(stderr, "|___________________________________"
				"_________________________________|\n");
		} else {
			size_t x = snprintf(json_output + pos, length,
					    "    \"test\": {\n");
			length -= x;
			pos += x;
			x = snprintf(json_output + pos, length,
				     "        \"slab size, bytes\": \"%u\",\n",
				     slab_size);
			length -= x;
			pos += x;
		}
		small_alloc_basic(slab_size);
		if (!no_large_test) {
			quota_init(&quota, UINT_MAX);
			slab_arena_create(&arena, &quota, 0, slab_size, MAP_PRIVATE);
			slab_cache_create(&cache, &arena);
			small_alloc_large();
			slab_cache_destroy(&cache);
			slab_arena_destroy(&arena);
		}
		if (!human) {
			x = snprintf(json_output + pos, length,
				     "    }%s\n",
				     (slab_size == SLAB_SIZE_MAX ? "" : ","));
			length -= x;
			pos += x;
		}
	}
	if (!human) {
		x = snprintf (json_output + pos, length, "}\n");
		fprintf(stderr, "%s\n", json_output);
	}
	return EXIT_SUCCESS;
}
