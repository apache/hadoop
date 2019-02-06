/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef LIBHDFS_NATIVE_TESTS_EXPECT_H
#define LIBHDFS_NATIVE_TESTS_EXPECT_H

#include <inttypes.h>
#include <stdio.h>

struct hdfsFile_internal;

#define EXPECT_ZERO(x) \
    do { \
        int __my_ret__ = x; \
        if (__my_ret__) { \
            int __my_errno__ = errno; \
            fprintf(stderr, "TEST_ERROR: failed on %s:%d with return " \
		    "code %d (errno: %d): got nonzero from %s\n", \
		    __FILE__, __LINE__, __my_ret__, __my_errno__, #x); \
            return __my_ret__; \
        } \
    } while (0);

#define EXPECT_NULL(x) \
    do { \
        const void* __my_ret__ = x; \
        int __my_errno__ = errno; \
        if (__my_ret__ != NULL) { \
            fprintf(stderr, "TEST_ERROR: failed on %s:%d (errno: %d): " \
		    "got non-NULL value %p from %s\n", \
		    __FILE__, __LINE__, __my_errno__, __my_ret__, #x); \
            return -1; \
        } \
    } while (0);

#define EXPECT_NULL_WITH_ERRNO(x, e) \
    do { \
        const void* __my_ret__ = x; \
        int __my_errno__ = errno; \
        if (__my_ret__ != NULL) { \
            fprintf(stderr, "TEST_ERROR: failed on %s:%d (errno: %d): " \
		    "got non-NULL value %p from %s\n", \
		    __FILE__, __LINE__, __my_errno__, __my_ret__, #x); \
            return -1; \
        } \
        if (__my_errno__ != e) { \
            fprintf(stderr, "TEST_ERROR: failed on %s:%d (errno: %d): " \
		    "got expected NULL without expected errno %d from %s\n", \
		    __FILE__, __LINE__, __my_errno__, e, #x); \
            return -1; \
        } \
    } while (0);

#define EXPECT_NONNULL(x) \
    do { \
        const void* __my_ret__ = x; \
        int __my_errno__ = errno; \
        if (__my_ret__ == NULL) { \
            fprintf(stderr, "TEST_ERROR: failed on %s:%d (errno: %d): " \
		    "got NULL from %s\n", __FILE__, __LINE__, __my_errno__, #x); \
            return -1; \
        } \
    } while (0);

#define EXPECT_NEGATIVE_ONE_WITH_ERRNO(x, e) \
    do { \
        int __my_ret__ = x; \
        int __my_errno__ = errno; \
        if (__my_ret__ != -1) { \
            fprintf(stderr, "TEST_ERROR: failed on %s:%d with return " \
                "code %d (errno: %d): expected -1 from %s\n", \
                    __FILE__, __LINE__, \
                __my_ret__, __my_errno__, #x); \
            return -1; \
        } \
        if (__my_errno__ != e) { \
            fprintf(stderr, "TEST_ERROR: failed on %s:%d with return " \
                "code %d (errno: %d): expected errno = %d from %s\n", \
                __FILE__, __LINE__, __my_ret__, __my_errno__, e, #x); \
            return -1; \
	} \
    } while (0);

#define EXPECT_NONZERO(x) \
    do { \
        int __my_ret__ = x; \
        int __my_errno__ = errno; \
        if (!__my_ret__) { \
            fprintf(stderr, "TEST_ERROR: failed on %s:%d with return " \
              "code %d (errno: %d): got zero from %s\n", __FILE__, __LINE__, \
              __my_ret__, __my_errno__, #x); \
            return -1; \
        } \
    } while (0);

#define EXPECT_NONNEGATIVE(x) \
    do { \
        int __my_ret__ = x; \
        int __my_errno__ = errno; \
        if (__my_ret__ < 0) { \
            fprintf(stderr, "TEST_ERROR: failed on %s:%d with return " \
                "code %d (errno: %d): got negative return from %s\n", \
                __FILE__, __LINE__, __my_ret__, __my_errno__, #x); \
            return __my_ret__; \
        } \
    } while (0);

#define EXPECT_INT_EQ(x, y) \
    do { \
        int __my_ret__ = y; \
        int __my_errno__ = errno; \
        if (__my_ret__ != (x)) { \
            fprintf(stderr, "TEST_ERROR: failed on %s:%d with return " \
              "code %d (errno: %d): expected %d\n", \
               __FILE__, __LINE__, __my_ret__, __my_errno__, (x)); \
            return -1; \
        } \
    } while (0);

#define EXPECT_INT_LT(x, y) \
    do { \
        int __my_ret__ = x; \
        int __my_errno__ = errno; \
        if (__my_ret__ >= (y)) { \
            fprintf(stderr, "TEST_ERROR: failed on %s:%d with return " \
              "code %d (errno: %d): expected less than %d\n", \
               __FILE__, __LINE__, __my_ret__, __my_errno__, (y)); \
            return -1; \
        } \
    } while (0);

#define EXPECT_INT_LE(x, y) \
    do { \
        int __my_ret__ = x; \
        int __my_errno__ = errno; \
        if (__my_ret__ > (y)) { \
            fprintf(stderr, "TEST_ERROR: failed on %s:%d with return " \
              "code %d (errno: %d): expected less than or equal %d\n", \
               __FILE__, __LINE__, __my_ret__, __my_errno__, (y)); \
            return -1; \
        } \
    } while (0);

#define EXPECT_INT_GT(x, y) \
    do { \
        int __my_ret__ = x; \
        int __my_errno__ = errno; \
        if (__my_ret__ <= (y)) { \
            fprintf(stderr, "TEST_ERROR: failed on %s:%d with return " \
              "code %d (errno: %d): expected greater than %d\n", \
               __FILE__, __LINE__, __my_ret__, __my_errno__, (y)); \
            return -1; \
        } \
    } while (0);

#define EXPECT_INT_GE(x, y) \
    do { \
        int __my_ret__ = x; \
        int __my_errno__ = errno; \
        if (__my_ret__ < (y)) { \
            fprintf(stderr, "TEST_ERROR: failed on %s:%d with return " \
              "code %d (errno: %d): expected greater than or equal %d\n", \
               __FILE__, __LINE__, __my_ret__, __my_errno__, (y)); \
            return -1; \
        } \
    } while (0);

#define EXPECT_INT64_EQ(x, y) \
    do { \
        int64_t __my_ret__ = y; \
        int __my_errno__ = errno; \
        if (__my_ret__ != (x)) { \
            fprintf(stderr, "TEST_ERROR: failed on %s:%d with return " \
              "value %"PRId64" (errno: %d): expected %"PRId64"\n", \
               __FILE__, __LINE__, __my_ret__, __my_errno__, (x)); \
            return -1; \
        } \
    } while (0);

#define ASSERT_INT64_EQ(x, y) \
    do { \
        int64_t __my_ret__ = y; \
        int __my_errno__ = errno; \
        if (__my_ret__ != (x)) { \
            fprintf(stderr, "TEST_ERROR: failed on %s:%d with return " \
              "value %"PRId64" (errno: %d): expected %"PRId64"\n", \
               __FILE__, __LINE__, __my_ret__, __my_errno__, (x)); \
            exit(EXIT_FAILURE); \
        } \
    } while (0);

#define EXPECT_UINT64_EQ(x, y) \
    do { \
        uint64_t __my_ret__ = y; \
        int __my_errno__ = errno; \
        if (__my_ret__ != (x)) { \
            fprintf(stderr, "TEST_ERROR: failed on %s:%d with return " \
              "value %"PRIu64" (errno: %d): expected %"PRIu64"\n", \
               __FILE__, __LINE__, __my_ret__, __my_errno__, (x)); \
            return -1; \
        } \
    } while (0);

#define RETRY_ON_EINTR_GET_ERRNO(ret, expr) do { \
    ret = expr; \
    if (!ret) \
        break; \
    ret = -errno; \
    } while (ret == -EINTR);

#define EXPECT_STR_CONTAINS(str, substr) \
    do { \
        char *_my_ret_ = (str); \
        int _my_errno_ = errno; \
        if ((str) == NULL) { \
            fprintf(stderr, "TEST_ERROR: failed on %s:%d with NULL return " \
              "return value (errno: %d): expected substring: %s\n", \
              __FILE__, __LINE__, _my_errno_, (substr)); \
            return -1; \
        } \
        if (strstr((str), (substr)) == NULL) { \
            fprintf(stderr, "TEST_ERROR: failed on %s:%d with return " \
              "value %s (errno: %d): expected substring: %s\n", \
              __FILE__, __LINE__, _my_ret_, _my_errno_, (substr)); \
            return -1; \
        } \
    } while (0);

/**
 * Test that an HDFS file has the given statistics.
 *
 * Any parameter can be set to UINT64_MAX to avoid checking it.
 *
 * @return 0 on success; error code otherwise
 */
int expectFileStats(struct hdfsFile_internal *file,
      uint64_t expectedTotalBytesRead,
      uint64_t expectedTotalLocalBytesRead,
      uint64_t expectedTotalShortCircuitBytesRead,
      uint64_t expectedTotalZeroCopyBytesRead);

#endif
