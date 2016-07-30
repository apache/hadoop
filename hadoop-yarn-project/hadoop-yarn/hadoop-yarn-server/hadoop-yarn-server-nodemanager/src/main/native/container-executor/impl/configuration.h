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

#include <stddef.h>

/** Define a platform-independent constant instead of using PATH_MAX */

#define EXECUTOR_PATH_MAX 4096

/**
 * Ensure that the configuration file and all of the containing directories
 * are only writable by root. Otherwise, an attacker can change the
 * configuration and potentially cause damage.
 * returns 0 if permissions are ok
 */
int check_configuration_permissions(const char* file_name);

/**
 * Return a string with the configuration file path name resolved via realpath(3)
 *
 * NOTE: relative path names are resolved relative to the second argument not getwd(3)
 */
char *resolve_config_path(const char* file_name, const char *root);

// read the given configuration file
void read_config(const char* config_file);

//method exposed to get the configurations
char *get_value(const char* key);

//function to return array of values pointing to the key. Values are
//comma seperated strings.
char ** get_values(const char* key);

// Extracts array of values from the comma separated list of values.
char ** extract_values(char *value);

// free the memory returned by get_values
void free_values(char** values);

//method to free allocated configuration
void free_configurations();

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
