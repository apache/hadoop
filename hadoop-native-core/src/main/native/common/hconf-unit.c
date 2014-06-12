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
#include "common/hconf.h"
#include "config.h"
#include "test/test.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>

const char* const TEST_XML_NAMES[] = {
    "core-default.xml",
    "core-site.xml",
    "hdfs-default.xml",
    "hdfs-site.xml",
    NULL
};

static int test_hconf_builder_free(void)
{
    struct hconf_builder *bld = NULL;

    EXPECT_NULL(hconf_builder_alloc(&bld));
    EXPECT_NONNULL(bld);
    hconf_builder_free(bld);
    bld = NULL;
    EXPECT_NULL(hconf_builder_alloc(&bld));
    EXPECT_NONNULL(bld);
    hconf_builder_set(bld, "foo", "bar");
    hconf_builder_free(bld);
    return 0;
}

static int test_hconf_create(void)
{
    struct hconf_builder *bld = NULL;
    struct hconf *conf = NULL;
    int32_t i32 = 0;
    int64_t i64 = 0;
    double g = 0;

    EXPECT_NULL(hconf_builder_alloc(&bld));
    EXPECT_NONNULL(bld);
    hconf_builder_set(bld, "foo", "foo_val");
    hconf_builder_set(bld, "bar", "123");
    hconf_builder_set(bld, "baz", "1.25");
    hconf_builder_set(bld, "foo", "foo_val2");
    hconf_builder_set(bld, "nothing", "");
    EXPECT_NO_HADOOP_ERR(hconf_build(bld, &conf));
    EXPECT_NONNULL(conf);
    EXPECT_STR_EQ("foo_val2", hconf_get(conf, "foo"));
    EXPECT_INT_ZERO(hconf_get_int32(conf, "bar", &i32));
    EXPECT_INT_EQ(123, i32);
    EXPECT_INT_ZERO(hconf_get_int64(conf, "bar", &i64));
    EXPECT_INT64_EQ((int64_t)123, i64);
    EXPECT_INT_ZERO(hconf_get_float64(conf, "baz", &g));
    EXPECT_NULL(hconf_get(conf, "nothing"));
    EXPECT_NULL(hconf_get(conf, "nada"));
    if (g != 1.25) {
        fail("got bad value for baz: expected %g; got %g", 1.25, g);
    }
    hconf_free(conf);
    return 0;
}

static int test_hconf_substitutions(void)
{
    struct hconf_builder *bld = NULL;
    struct hconf *conf = NULL;

    EXPECT_NULL(hconf_builder_alloc(&bld));
    EXPECT_NONNULL(bld);
    hconf_builder_set(bld, "foo.bar", "foobar");
    hconf_builder_set(bld, "foo.bar.indirect", "3${foo.bar}");
    hconf_builder_set(bld, "foo.bar.double.indirect", "2${foo.bar.indirect}");
    hconf_builder_set(bld, "foo.bar.triple.indirect", "1${foo.bar.double.indirect}");
    hconf_builder_set(bld, "foo.baz", "${foo.bar}");
    hconf_builder_set(bld, "foo.unresolved", "${foo.nonexistent}");
    hconf_builder_set(bld, "double.foo.bar", "${foo.bar}${foo.bar}");
    hconf_builder_set(bld, "double.foo.bar.two", "Now ${foo.bar} and ${foo.bar}");
    hconf_builder_set(bld, "expander", "a${expander}");
    hconf_builder_set(bld, "tweedledee", "${tweedledum}");
    hconf_builder_set(bld, "tweedledum", "${tweedledee}");
    hconf_builder_set(bld, "bling", "{$$$${$$${$$$$$$$");
    EXPECT_NO_HADOOP_ERR(hconf_build(bld, &conf));
    EXPECT_NONNULL(conf);
    EXPECT_STR_EQ("foobar", hconf_get(conf, "foo.bar"));
    EXPECT_STR_EQ("123foobar", hconf_get(conf, "foo.bar.triple.indirect"));
    EXPECT_STR_EQ("aaaaaaaaaaaaaaaaaaaaa${expander}",
                  hconf_get(conf, "expander"));
    EXPECT_STR_EQ("foobar", hconf_get(conf, "foo.baz"));
    EXPECT_STR_EQ("${foo.nonexistent}", hconf_get(conf, "foo.unresolved"));
    EXPECT_STR_EQ("foobarfoobar", hconf_get(conf, "double.foo.bar"));
    EXPECT_STR_EQ("Now foobar and foobar",
                  hconf_get(conf, "double.foo.bar.two"));
    EXPECT_STR_EQ("${tweedledee}", hconf_get(conf, "tweedledee"));
    EXPECT_STR_EQ("{$$$${$$${$$$$$$$", hconf_get(conf, "bling"));
    hconf_free(conf);
    return 0;
}

static int test_hconf_xml(void)
{
    struct hconf_builder *bld = NULL;
    struct hconf *conf = NULL;

    EXPECT_NULL(hconf_builder_alloc(&bld));
    EXPECT_NONNULL(bld);
    EXPECT_NO_HADOOP_ERR(hconf_builder_load_xmls(bld, TEST_XML_NAMES,
            HCONF_XML_TEST_PATH ":" HCONF_XML_TEST_PATH "/.."));
    EXPECT_NO_HADOOP_ERR(hconf_build(bld, &conf));
    EXPECT_NONNULL(conf);
    EXPECT_NULL(hconf_get(conf, "foo.empty"));
    EXPECT_STR_EQ("1", hconf_get(conf, "foo.final"));
    EXPECT_STR_EQ("hdfs-site-val", hconf_get(conf, "foo.overridden"));
    EXPECT_STR_EQ("woo:hdfs-default.woo:hdfs-default.hdfs-default.",
                  hconf_get(conf, "triple.foo.bar"));
    hconf_free(conf);
    return 0;
}

int main(void)
{
    EXPECT_INT_ZERO(test_hconf_builder_free());
    EXPECT_INT_ZERO(test_hconf_create());
    EXPECT_INT_ZERO(test_hconf_substitutions());
    EXPECT_INT_ZERO(test_hconf_xml());

    return EXIT_SUCCESS;
}

// vim: ts=4:sw=4:tw=79:et
