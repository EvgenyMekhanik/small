/*
 * Copyright 2010-2020, Tarantool AUTHORS, please see AUTHORS file.
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
#include "small/small_class.h"
#include "unit.h"
#include <math.h>

static void
test_visual(void)
{
	header();
	plan(0);

	struct small_class sc;
	small_class_create(&sc, 2, 1.2, 12);
	printf("desired factor %f, actual factor %f\n",
	       sc.requested_factor, sc.actual_factor);

	printf("  sz   cls cls_sz real_factor\n");
	for (unsigned i = 0; i <= 100; i++) {
		unsigned cls = class_by_size(&sc, i);
		unsigned cls_sz = size_by_class(&sc, cls);
		unsigned cls_sz_next = size_by_class(&sc, cls + 1);
		double real_factor = 1. * cls_sz_next / cls_sz;
		printf("%3u   %3u   %3u    %lf\n", i, cls, cls_sz, real_factor);
	}

	check_plan();
	footer();
}

static void
check_expectation()
{
	header();

	const unsigned test_sizes = 1024;
	const unsigned test_classes = 1024;
	/* we expect 4 effective bits with factor = 1.05 */
	float factor = 1.05;
	const unsigned eff_size = 16;
	unsigned test_class_size[test_classes + eff_size];

	plan(4 * (1 + 2 * (test_sizes + 1)));

	for (int variant = 0; variant < 4; variant++) {
		unsigned granularity = (variant & 1) != 0 ? 1 : 4;
		unsigned min_alloc = granularity + ((variant & 2) != 0 ? 0 : 10);

		{
			unsigned class_size = min_alloc - granularity;
			/* incremental growth */
			for (unsigned i = 0; i < eff_size; i++) {
				class_size += granularity;
				test_class_size[i] = class_size;
			}
			/* exponential growth */
			unsigned growth = granularity;
			for (unsigned i = eff_size; i < test_classes; i += eff_size) {
				for (unsigned j = 0; j < eff_size; j++) {
					class_size += growth;
					test_class_size[i + j] = class_size;
				}
				growth *= 2;
			}
		}

		struct small_class sc;
		small_class_create(&sc, granularity, factor, min_alloc);
		is(sc.eff_size, eff_size, "unexpected eff_size");

		for (unsigned s = 0; s <= test_sizes; s++) {
			unsigned expect_class = 0;
			while (expect_class < test_classes && s > test_class_size[expect_class])
				expect_class++;
			unsigned expect_class_size = test_class_size[expect_class];
			unsigned got_class = class_by_size(&sc, s);
			unsigned got_class_size = size_by_class(&sc, got_class);

			is(got_class, expect_class, "unexpected size class");
			is(got_class_size, expect_class_size, "unexpected class size");
		}
	}

	check_plan();
	footer();
}

static void
check_factor()
{
	header();

	plan(2 * 99 * 4);

	for (unsigned granularity = 1; granularity <= 4; granularity *= 4) {
		for(float factor = 1.01; factor < 1.995; factor += 0.01) {
			struct small_class sc;
			small_class_create(&sc, granularity, factor, granularity);
			float k = powf(factor, 0.5f);
			ok(sc.actual_factor >= factor / k, "wrong actual factor (1)");
			ok(sc.actual_factor <= factor * k, "wrong actual factor (1)");

			float min_deviation = 1.f;
			float max_deviation = 1.f;
			/* Skip incremental growth. */
			for (unsigned i = sc.eff_size; i < sc.eff_size*3; i++) {
				unsigned cl_sz1 = size_by_class(&sc, i);
				unsigned cl_sz2 = size_by_class(&sc, i + 1);
				float real_growth = 1.f * cl_sz2 / cl_sz1;
				float deviation = sc.actual_factor / real_growth;
				if (deviation < min_deviation)
					min_deviation = deviation;
				if (deviation > max_deviation)
					max_deviation = deviation;
			}
			float ln2 = logf(2);
			ok(min_deviation > ln2, "wrong approximation (1)");
			ok(max_deviation < 2 * ln2, "wrong approximation (2)");
		}
	}

	check_plan();
	footer();
}


int
main(void)
{
	header();
	plan(3);

	test_visual();
	check_expectation();
	check_factor();

	int rc = check_plan();
	footer();
	return rc;
}
