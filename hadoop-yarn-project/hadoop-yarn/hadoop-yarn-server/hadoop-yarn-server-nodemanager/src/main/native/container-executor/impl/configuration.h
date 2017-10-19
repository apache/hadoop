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

#ifndef __YARN_CONTAINER_EXECUTOR_CONFIG_H__
#define __YARN_CONTAINER_EXECUTOR_CONFIG_H__

#ifdef __FreeBSD__
#define _WITH_GETLINE
#endif

#include "config.h"

#define CONF_FILENAME "container-executor.cfg"

// When building as part of a Maven build this value gets defined by using
// container-executor.conf.dir property. See:
//   hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/pom.xml
// for details.
// NOTE: if this ends up being a relative path it gets resolved relative to
//       the location of the container-executor binary itself, not getwd(3)
#ifndef HADOOP_CONF_DIR
#error HADOOP_CONF_DIR must be defined
#endif

#include <stddef.h>

// Configuration data structures.
struct kv_pair {
  const char *key;
  const char *value;
};

struct section {
  int size;
  char *name;
  struct kv_pair **kv_pairs;
};

struct configuration {
  int size;
  struct section **sections;
};

/**
 * Function to ensure that the configuration file and all of the containing
 * directories are only writable by root. Otherwise, an attacker can change
 * the configuration and potentially cause damage.
 *
 * @param file_name name of the config file
 *
 * @returns 0 if permissions are correct, non-zero on error
 */
int check_configuration_permissions(const char *file_name);

/**
 * Return a string with the configuration file path name resolved via
 * realpath(3). Relative path names are resolved relative to the second
 * argument and not getwd(3). It's up to the caller to free the returned
 * value.
 *
 * @param file_name name of the config file
 * @param root the path against which relative path names are to be resolved
 *
 * @returns the resolved configuration file path
 */
char* resolve_config_path(const char *file_name, const char *root);

/**
 * Read the given configuration file into the specified configuration struct.
 * It's the responsibility of the caller to call free_configurations to free
 * the allocated memory. The function will check to ensure that the
 * configuration file has the appropriate owner and permissions.
 *
 * @param file_path name of the configuration file to be read
 * @param cfg the configuration structure to be filled.
 *
 * @return 0 on success, non-zero if there was an error
 */
int read_config(const char *file_path, struct configuration *cfg);

/**
 * Get the value for a key in the specified section. It's up to the caller to
 * free the memory used for storing the return value.
 *
 * @param key key the name of the key
 * @param section the section to be looked up
 *
 * @return pointer to the value if the key was found, null otherwise
 */
char* get_section_value(const char *key, const struct section *section);

/**
 * Function to get the values for a given key in the specified section.
 * The value is split by ",". It's up to the caller to free the memory used
 * for storing the return values.
 *
 * @param key the key to be looked up
 * @param section the section to be looked up
 *
 * @return array of values, null if the key was not found
 */
char** get_section_values(const char *key, const struct section *section);

/**
 * Function to get the values for a given key in the specified section.
 * The value is split by the specified delimiter. It's up to the caller to
 * free the memory used for storing the return values.
 *
 * @param key the key to be looked up
 * @param section the section to be looked up
 * @param delimiter the delimiter to be used to split the value
 *
 * @return array of values, null if the key was not found
 */
char** get_section_values_delimiter(const char *key, const struct section *section,
    const char *delim);

/**
 * Get the value for a key in the specified section in the specified
 * configuration. It's up to the caller to free the memory used for storing
 * the return value.
 *
 * @param key key the name of the key
 * @param section the name section to be looked up
 * @param cfg the configuration to be used
 *
 * @return pointer to the value if the key was found, null otherwise
 */
char* get_configuration_value(const char *key, const char* section,
    const struct configuration *cfg);

/**
 * Function to get the values for a given key in the specified section in the
 * specified configuration. The value is split by ",". It's up to the caller to
 * free the memory used for storing the return values.
 *
 * @param key the key to be looked up
 * @param section the name of the section to be looked up
 * @param cfg the configuration to be looked up
 *
 * @return array of values, null if the key was not found
 */
char** get_configuration_values(const char *key, const char* section,
    const struct configuration *cfg);

/**
 * Function to get the values for a given key in the specified section in the
 * specified configuration. The value is split by the specified delimiter.
 * It's up to the caller to free the memory used for storing the return values.
 *
 * @param key the key to be looked up
 * @param section the name of the section to be looked up
 * @param cfg the section to be looked up
 * @param delimiter the delimiter to be used to split the value
 *
 * @return array of values, null if the key was not found
 */
char** get_configuration_values_delimiter(const char *key, const char* section,
    const struct configuration *cfg, const char *delimiter);

/**
 * Function to retrieve the specified section from the configuration.
 *
 * @param section the name of the section to retrieve
 * @param cfg the configuration structure to use
 *
 * @return pointer to section struct containing details of the section
 *         null on error
 */
struct section* get_configuration_section(const char *section,
    const struct configuration *cfg);

/**
 * Method to free an allocated config struct.
 *
 * @param cfg pointer to the structure to free
 */
void free_configuration(struct configuration *cfg);

/**
 * If str is a string of the form key=val, find 'key'
 *
 * @param input    The input string
 * @param out      Where to put the output string.
 * @param out_len  The length of the output buffer.
 *
 * @return         -ENAMETOOLONG if out_len is not long enough;
 *                 -EINVAL if there is no equals sign in the input;
 *                 0 on success
 */
int get_kv_key(const char *input, char *out, size_t out_len);

/**
 * If str is a string of the form key=val, find 'val'
 *
 * @param input    The input string
 * @param out      Where to put the output string.
 * @param out_len  The length of the output buffer.
 *
 * @return         -ENAMETOOLONG if out_len is not long enough;
 *                 -EINVAL if there is no equals sign in the input;
 *                 0 on success
 */
int get_kv_value(const char *input, char *out, size_t out_len);

char *get_config_path(const char* argv0);

#endif
