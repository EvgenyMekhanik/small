#include <small/quota.h>
#include <small/ibuf.h>
#include <small/slab_cache.h>
#include <stdio.h>
#include "unit.h"

struct slab_cache cache;
struct slab_arena arena;
struct quota quota;

void
ibuf_basic()
{
	header();

	struct ibuf ibuf;

	ibuf_create(&ibuf, &cache, 16320);

	fail_unless(ibuf_used(&ibuf) == 0);

	void *ptr = ibuf_alloc(&ibuf, 10);

	fail_unless(ptr);

	fail_unless(ibuf_used(&ibuf) == 10);

	ptr = ibuf_alloc(&ibuf, 1000000);
	fail_unless(ptr);

	fail_unless(ibuf_used(&ibuf) == 1000010);

	ibuf_reset(&ibuf);

	fail_unless(ibuf_used(&ibuf) == 0);

	ptr = ibuf_alloc(&ibuf, UINT32_MAX);
	fail_unless(ptr != NULL);
	fail_unless(ibuf_used(&ibuf) == UINT32_MAX);

	ibuf_reset(&ibuf);

	fail_unless(ibuf_used(&ibuf) == 0);

	footer();
}

int main()
{
	quota_init(&quota, QUOTA_MAX);
	slab_arena_create(&arena, &quota, 0,
			  4000000, MAP_PRIVATE);
	slab_cache_create(&cache, &arena);

	ibuf_basic();

	slab_cache_destroy(&cache);
	slab_arena_destroy(&arena);
}
