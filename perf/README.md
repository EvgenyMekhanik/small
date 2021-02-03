# peformance test for small allocator

To build the test, you need to install google benchmark.
You can get detailed instructions here https://github.com/google/benchmark.
To compare performance, it is useful to use compare.py utility from google
benchmark https://github.com/google/benchmark/blob/master/docs/tools.md. But
be careful, if the test failed with an error, the utility will assume that the
execution time of the operation under test is zero.
