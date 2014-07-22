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
#include "common/uri.h"
#include "test/test.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <uv.h>

static int check_uri_params(const char *scheme, const char *escheme,
                            const char *user, const char *euser,
                            const char *auth, const char *eauth,
                            int port, int eport,
                            const char *path, const char *epath)
{
    EXPECT_STR_EQ(escheme, scheme);
    EXPECT_STR_EQ(euser, user);
    EXPECT_STR_EQ(eauth, auth);
    EXPECT_STR_EQ(epath, path);
    EXPECT_INT_EQ(eport, port);
    return 0;
}

static struct hadoop_err *test_parse_uri(const char *uri_str,
            const char *escheme, const char *euser_info, const char *eauth,
            int eport, const char *epath)
{
    struct hadoop_uri *base = NULL, *uri = NULL;
    struct hadoop_err *err = NULL;

    err = hadoop_uri_parse("hdfs:///home/cmccabe/", NULL, &base,
                H_URI_APPEND_SLASH | H_URI_PARSE_ALL);
    if (err)
        goto done;
    err = hadoop_uri_parse(uri_str, base, &uri, H_URI_PARSE_ALL);
    if (err)
        goto done;
    if (check_uri_params(uri->scheme, escheme,
                         uri->user_info, euser_info,
                         uri->auth, eauth, uri->port, eport,
                         uri->path, epath)) {
        err = hadoop_lerr_alloc(EINVAL, "check_uri_params: failed.");
        if (err)
            goto done;
    }
    err = NULL;

done:
    hadoop_uri_free(base);
    hadoop_uri_free(uri);
    return err;
}

int main(void)
{
    struct hadoop_err *err;

    //EXPECT_NO_HADOOP_ERR(test_parse_uri("localhost:6000"));
    EXPECT_NO_HADOOP_ERR(test_parse_uri("a/b/c/d",
                    "hdfs", "", "", 0, "/home/cmccabe/a/b/c/d"));
    EXPECT_NO_HADOOP_ERR(test_parse_uri("hdfs://localhost:6000",
                    "hdfs", "", "localhost", 6000, ""));
    EXPECT_NO_HADOOP_ERR(test_parse_uri("file:///a/b/c/d",
                    "file", "", "", 0, "/a/b/c/d"));
    EXPECT_NO_HADOOP_ERR(test_parse_uri("s3://jeffbezos:6000",
                    "s3", "", "jeffbezos", 6000, ""));
    EXPECT_NO_HADOOP_ERR(test_parse_uri("s3:///foo",
                    "s3", "", "", 0, "/foo"));
    EXPECT_NO_HADOOP_ERR(test_parse_uri("hdfs://nn1.example.com/foo/bar",
                    "hdfs", "", "nn1.example.com", 0, "/foo/bar"));
    EXPECT_NO_HADOOP_ERR(test_parse_uri("hdfs://user:password@hdfshost:9000/a/b/c",
                    "hdfs", "user:password", "hdfshost", 9000, "/a/b/c"));
    err = test_parse_uri("://user:password@hdfshost:9000/a/b/c",
                    "", "", "", 0, "");
    EXPECT_NONNULL(strstr(hadoop_err_msg(err), "failed to parse"));
    hadoop_err_free(err);

    return EXIT_SUCCESS;
}

// vim: ts=4:sw=4:tw=79:et
