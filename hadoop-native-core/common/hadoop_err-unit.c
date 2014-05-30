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

#include "common/hadoop_err.h"

#include "common/test.h"

#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <uv.h>

#define RUNTIME_EXCEPTION_ERROR_CODE EFAULT

static int hadoop_lerr_alloc_test(int code, char *verMsg, char *fmt) {
	struct hadoop_err *err;
	err = hadoop_lerr_alloc(code, fmt);
	EXPECT_STR_EQ(verMsg, hadoop_err_msg(err));
	EXPECT_INT_EQ(code, hadoop_err_code(err));
	hadoop_err_free(err);
	return 0;
}

static int hadoop_lerr_alloc_test2(int code, char *verMsg) {
	struct hadoop_err *err;
	char msg[100];
	memset(msg, 0, 100);
	strcat(msg, verMsg);
	err = hadoop_lerr_alloc(code, "foo bar baz %d", 101);
	EXPECT_STR_EQ(strcat(msg, "foo bar baz 101"), hadoop_err_msg(err));
	EXPECT_INT_EQ(code, hadoop_err_code(err));
	hadoop_err_free(err);
	return 0;
}

static int hadoop_uverr_alloc_test(int code, char *verMsg, char *fmt) {
	struct hadoop_err *err;
	err = hadoop_uverr_alloc(code, fmt);
	EXPECT_STR_EQ(verMsg, hadoop_err_msg(err));
	EXPECT_INT_EQ(code, hadoop_err_code(err));
	hadoop_err_free(err);
	return 0;
}

static int hadoop_uverr_alloc_test2(int code, char *verMsg) {
	struct hadoop_err *err;
	char msg[100];
	memset(msg, 0, 100);
	strcat(msg, verMsg);
	err = hadoop_uverr_alloc(code, "foo bar baz %d", 101);
	EXPECT_STR_EQ(strcat(msg, "foo bar baz 101"), hadoop_err_msg(err));
	EXPECT_INT_EQ(code, hadoop_err_code(err));
	hadoop_err_free(err);
	return 0;
}

int main(void) {
	hadoop_lerr_alloc_test(RUNTIME_EXCEPTION_ERROR_CODE,
			"org.apache.hadoop.native.HadoopCore.RuntimeException: "
					"test RUNTIME_EXCEPTION_ERROR_CODE",
			"test RUNTIME_EXCEPTION_ERROR_CODE");
	hadoop_lerr_alloc_test(EINVAL,
			"org.apache.hadoop.native.HadoopCore.InvalidRequestException: "
					"test EINVAL", "test EINVAL");
	hadoop_lerr_alloc_test(ENOMEM,
			"org.apache.hadoop.native.HadoopCore.OutOfMemoryException: "
					"test ENOMEM", "test ENOMEM");
	hadoop_lerr_alloc_test(0,
			"org.apache.hadoop.native.HadoopCore.IOException: "
					"test default", "test default");
	hadoop_uverr_alloc_test(UV_EOF,
			"org.apache.hadoop.native.HadoopCore.EOFException: end of file: "
					"test UV_EOF", "test UV_EOF");
	hadoop_uverr_alloc_test(UV_EINVAL,
			"org.apache.hadoop.native.HadoopCore.InvalidRequestException: "
					"invalid argument: test UV_EINVAL", "test UV_EINVAL");
	hadoop_uverr_alloc_test(UV_ECONNREFUSED,
			"org.apache.hadoop.native.HadoopCore.ConnectionRefusedException: "
					"connection refused: test UV_ECONNREFUSED",
			"test UV_ECONNREFUSED");
	hadoop_uverr_alloc_test(UV_ENOMEM,
			"org.apache.hadoop.native.HadoopCore.OutOfMemoryException: "
					"not enough memory: test UV_ENOMEM", "test UV_ENOMEM");
	hadoop_uverr_alloc_test(0,
			"org.apache.hadoop.native.HadoopCore.IOException: "
					"Unknown system error: test default", "test default");
	hadoop_lerr_alloc_test2(EINVAL,
			"org.apache.hadoop.native.HadoopCore.InvalidRequestException: ");
	hadoop_lerr_alloc_test2(RUNTIME_EXCEPTION_ERROR_CODE,
			"org.apache.hadoop.native.HadoopCore.RuntimeException: ");
	hadoop_lerr_alloc_test2(ENOMEM,
			"org.apache.hadoop.native.HadoopCore.OutOfMemoryException: ");
	hadoop_lerr_alloc_test2(0,
			"org.apache.hadoop.native.HadoopCore.IOException: ");
	hadoop_uverr_alloc_test2(UV_EOF,
			"org.apache.hadoop.native.HadoopCore.EOFException: end of file: ");
	hadoop_uverr_alloc_test2(UV_EINVAL,
			"org.apache.hadoop.native.HadoopCore.InvalidRequestException: "
					"invalid argument: ");
	hadoop_uverr_alloc_test2(UV_ECONNREFUSED,
			"org.apache.hadoop.native.HadoopCore.ConnectionRefusedException: "
					"connection refused: ");
	hadoop_uverr_alloc_test2(UV_ENOMEM,
			"org.apache.hadoop.native.HadoopCore.OutOfMemoryException: "
					"not enough memory: ");
	hadoop_uverr_alloc_test2(0,
			"org.apache.hadoop.native.HadoopCore.IOException: "
					"Unknown system error: ");
	return EXIT_SUCCESS;
}

// vim: ts=4:sw=4:tw=79:et
