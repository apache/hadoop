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

#ifndef HADOOP_CORE_COMMON_hconf
#define HADOOP_CORE_COMMON_hconf

#include <stdint.h>

struct hconf;
struct hconf_builder;

extern const char* const HDFS_XML_NAMES[];

/**
 * Allocate a new Hadoop configuration build object.
 *
 * @param out       (out param) The new Hadoop configuration builder object
 *
 * @return          NULL on success; the error otherwise.
 */
struct hadoop_err *hconf_builder_alloc(struct hconf_builder **out);

/**
 * Free a Hadoop configuration builder object.
 *
 * @param bld       The configuration builder object to free.
 */
void hconf_builder_free(struct hconf_builder *bld);

/**
 * Free a Hadoop configuration object.
 *
 * @param conf      The object to free.
 */
void hconf_free(struct hconf *conf);

/**
 * Set a Hadoop configuration string value.
 *
 * @param bld       The configuration builder object.
 * @param key       The configuration key.  Will be shallow-copied.
 * @param val       The configuration value.  Will be shallow-copied.
 */
void hconf_builder_set(struct hconf_builder *bld,
                const char *key, const char *val);

/**
 * Load a set of configuration XML files into the builder.
 *
 * @param bld       The configuration builder object.
 * @param XMLS      A NULL-terminated list of configuration XML files to read.
 * @param path      A semicolon-separated list of paths to search for the files
 *                      in XMLS.  This is essentially a JNI-style CLASSPATH.
 *                      (Like the JNI version, it doesn't support wildcards.)
 */
struct hadoop_err *hconf_builder_load_xmls(struct hconf_builder *bld,
                            const char * const* XMLS, const char *path);

/**
 * Build a hadoop configuration object.
 * Hadoop configuration objects are immutable.
 *
 * @param bld       The configuration builder object.  Will be freed, whether
 *                      or not the function succeeds.
 * @param conf      (out param) on success, the configuration object.
 *
 * @return          NULL on success; the hadoop error otherwise.
 */
struct hadoop_err *hconf_build(struct hconf_builder *bld,
                struct hconf **conf);

/**
 * Get a Hadoop configuration string value.
 *
 * @param conf      The configuration object.
 * @param key       The configuration key.
 *
 * @return          NULL if there was no such value.  The configuration value
 *                      otherwise.  This pointer will remain valid until the
 *                      enclosing configuration is freed.
 */
const char *hconf_get(struct hconf *conf, const char *key);

/**
 * Get a Hadoop configuration int32 value.
 *
 * @param conf      The configuration object.
 * @param key       The configuration key.
 * @param out       (out param) On success, the 32-bit value.
 *
 * @return          0 on success.
 *                  -ENOENT if there was no such key.
 */
int hconf_get_int32(struct hconf *conf, const char *key,
                            int32_t *out);

/**
 * Get a Hadoop configuration int64 value.
 *
 * @param conf      The configuration object.
 * @param key       The configuration key.
 * @param out       (out param) On success, the 64-bit value.
 *
 * @return          0 on success.
 *                  -ENOENT if there was no such key.
 */
int hconf_get_int64(struct hconf *conf, const char *key,
                            int64_t *out);

/**
 * Get a Hadoop configuration 64-bit float value.
 *
 * @param conf      The configuration object.
 * @param key       The configuration key.
 * @param out       (out param) On success, the 64-bit float value.
 *
 * @return          0 on success.
 *                  -ENOENT if there was no such key.
 */
int hconf_get_float64(struct hconf *conf, const char *key,
                              double *out);

#endif

// vim: ts=4:sw=4:tw=79:et
