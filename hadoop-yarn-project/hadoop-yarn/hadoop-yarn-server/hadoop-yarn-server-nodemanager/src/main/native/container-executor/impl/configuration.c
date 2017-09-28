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

// ensure we get the posix version of dirname by including this first
#include <libgen.h>

#include "configuration.h"
#include "util.h"
#include "get_executable.h"

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <errno.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#define MAX_SIZE 10

static const char COMMENT_BEGIN_CHAR = '#';
static const char SECTION_LINE_BEGIN_CHAR = '[';
static const char SECTION_LINE_END_CHAR = ']';

//clean up method for freeing section
void free_section(struct section *section) {
  int i = 0;
  for (i = 0; i < section->size; i++) {
    if (section->kv_pairs[i]->key != NULL) {
      free((void *) section->kv_pairs[i]->key);
    }
    if (section->kv_pairs[i]->value != NULL) {
      free((void *) section->kv_pairs[i]->value);
    }
    free(section->kv_pairs[i]);
  }
  if (section->kv_pairs) {
    free(section->kv_pairs);
    section->kv_pairs = NULL;
  }
  if (section->name) {
    free(section->name);
    section->name = NULL;
  }
  section->size = 0;
}

//clean up method for freeing configuration
void free_configuration(struct configuration *cfg) {
  int i = 0;
  for (i = 0; i < cfg->size; i++) {
    if (cfg->sections[i] != NULL) {
      free_section(cfg->sections[i]);
    }
  }
  if (cfg->sections) {
    free(cfg->sections);
  }
  cfg->size = 0;
}

/**
 * Is the file/directory only writable by root.
 * Returns 1 if true
 */
static int is_only_root_writable(const char *file) {
  struct stat file_stat;
  if (stat(file, &file_stat) != 0) {
    fprintf(ERRORFILE, "Can't stat file %s - %s\n", file, strerror(errno));
    return 0;
  }
  if (file_stat.st_uid != 0) {
    fprintf(ERRORFILE, "File %s must be owned by root, but is owned by %" PRId64 "\n",
            file, (int64_t) file_stat.st_uid);
    return 0;
  }
  if ((file_stat.st_mode & (S_IWGRP | S_IWOTH)) != 0) {
    fprintf(ERRORFILE,
            "File %s must not be world or group writable, but is %03lo\n",
            file, (unsigned long) file_stat.st_mode & (~S_IFMT));
    return 0;
  }
  return 1;
}

/**
 * Return a string with the configuration file path name resolved via realpath(3)
 *
 * NOTE: relative path names are resolved relative to the second argument not getwd(3)
 */
char *resolve_config_path(const char *file_name, const char *root) {
  const char *real_fname = NULL;
  char buffer[EXECUTOR_PATH_MAX * 2 + 1];

  if (file_name[0] == '/') {
    real_fname = file_name;
  } else if (realpath(root, buffer) != NULL) {
    strncpy(strrchr(buffer, '/') + 1, file_name, EXECUTOR_PATH_MAX);
    real_fname = buffer;
  }

#ifdef HAVE_CANONICALIZE_FILE_NAME
  char * ret = (real_fname == NULL) ? NULL : canonicalize_file_name(real_fname);
#else
  char *ret = (real_fname == NULL) ? NULL : realpath(real_fname, NULL);
#endif
#ifdef DEBUG
  fprintf(stderr,"ret = %s\n", ret);
  fprintf(stderr, "resolve_config_path(file_name=%s,root=%s)=%s\n",
          file_name, root ? root : "null", ret ? ret : "null");
#endif
  return ret;
}

/**
 * Ensure that the configuration file and all of the containing directories
 * are only writable by root. Otherwise, an attacker can change the
 * configuration and potentially cause damage.
 * returns 0 if permissions are ok
 */
int check_configuration_permissions(const char *file_name) {
  if (!file_name) {
    return -1;
  }

  // copy the input so that we can modify it with dirname
  char *dir = strdup(file_name);
  if (!dir) {
    fprintf(stderr, "Failed to make a copy of filename in %s.\n", __func__);
    return -1;
  }

  char *buffer = dir;
  do {
    if (!is_only_root_writable(dir)) {
      free(buffer);
      return -1;
    }
    dir = dirname(dir);
  } while (strcmp(dir, "/") != 0);
  free(buffer);
  return 0;
}

/**
 * Read a line from the the config file and return it without the newline.
 * The caller must free the memory allocated.
 */
static char *read_config_line(FILE *conf_file) {
  char *line = NULL;
  size_t linesize = 100000;
  ssize_t size_read = 0;
  size_t eol = 0;

  line = (char *) malloc(linesize);
  if (line == NULL) {
    fprintf(ERRORFILE, "malloc failed while reading configuration file.\n");
    exit(OUT_OF_MEMORY);
  }
  size_read = getline(&line, &linesize, conf_file);

  //feof returns true only after we read past EOF.
  //so a file with no new line, at last can reach this place
  //if size_read returns negative check for eof condition
  if (size_read == -1) {
    free(line);
    line = NULL;
    if (!feof(conf_file)) {
      fprintf(ERRORFILE, "Line read returned -1 without eof\n");
      exit(INVALID_CONFIG_FILE);
    }
  } else {
    eol = strlen(line) - 1;
    if (line[eol] == '\n') {
      //trim the ending new line
      line[eol] = '\0';
    }
  }
  return line;
}

/**
 * Return if the given line is a comment line.
 *
 * @param line the line to check
 *
 * @return 1 if the line is a comment line, 0 otherwise
 */
static int is_comment_line(const char *line) {
  if (line != NULL) {
    return (line[0] == COMMENT_BEGIN_CHAR);
  }
  return 0;
}

/**
 * Return if the given line is a section start line.
 *
 * @param line the line to check
 *
 * @return 1 if the line is a section start line, 0 otherwise
 */
static int is_section_start_line(const char *line) {
  size_t len = 0;
  if (line != NULL) {
    len = strlen(line) - 1;
    return (line[0] == SECTION_LINE_BEGIN_CHAR
            && line[len] == SECTION_LINE_END_CHAR);
  }
  return 0;
}

/**
 * Return the name of the section from the given section start line. The
 * caller must free the memory used.
 *
 * @param line the line to extract the section name from
 *
 * @return string with the name of the section, NULL otherwise
 */
static char *get_section_name(const char *line) {
  char *name = NULL;
  size_t len;

  if (is_section_start_line(line)) {
    // length of the name is the line - 2(to account for '[' and ']')
    len = strlen(line) - 2;
    name = (char *) malloc(len + 1);
    if (name == NULL) {
      fprintf(ERRORFILE, "malloc failed while reading section name.\n");
      exit(OUT_OF_MEMORY);
    }
    strncpy(name, line + sizeof(char), len);
    name[len] = '\0';
  }
  return name;
}

/**
 * Read an entry for the section from the line. Function returns 0 if an entry
 * was found, non-zero otherwise. Return values less than 0 indicate an error
 * with the config file.
 *
 * @param line the line to read the entry from
 * @param section the struct to read the entry into
 *
 * @return 0 if an entry was found
 *         <0 for config file errors
 *         >0 for issues such as empty line
 *
 */
static int read_section_entry(const char *line, struct section *section) {
  char *equaltok;
  char *temp_equaltok;
  const char *splitter = "=";
  char *buffer;
  size_t len = 0;
  if (line == NULL || section == NULL) {
    fprintf(ERRORFILE, "NULL params passed to read_section_entry");
    return -1;
  }
  len = strlen(line);
  if (len == 0) {
    return 1;
  }
  if ((section->size) % MAX_SIZE == 0) {
    section->kv_pairs = (struct kv_pair **) realloc(
        section->kv_pairs,
        sizeof(struct kv_pair *) * (MAX_SIZE + section->size));
    if (section->kv_pairs == NULL) {
      fprintf(ERRORFILE,
              "Failed re-allocating memory for configuration items\n");
      exit(OUT_OF_MEMORY);
    }
  }

  buffer = strdup(line);
  if (!buffer) {
    fprintf(ERRORFILE, "Failed to allocating memory for line, %s\n", __func__);
    exit(OUT_OF_MEMORY);
  }

  //tokenize first to get key and list of values.
  //if no equals is found ignore this line, can be an empty line also
  equaltok = strtok_r(buffer, splitter, &temp_equaltok);
  if (equaltok == NULL) {
    fprintf(ERRORFILE, "Error with line '%s', no '=' found\n", buffer);
    exit(INVALID_CONFIG_FILE);
  }
  section->kv_pairs[section->size] = (struct kv_pair *) malloc(
      sizeof(struct kv_pair));
  if (section->kv_pairs[section->size] == NULL) {
    fprintf(ERRORFILE, "Failed allocating memory for single section item\n");
    exit(OUT_OF_MEMORY);
  }
  memset(section->kv_pairs[section->size], 0,
         sizeof(struct kv_pair));
  section->kv_pairs[section->size]->key = trim(equaltok);

  equaltok = strtok_r(NULL, splitter, &temp_equaltok);
  if (equaltok == NULL) {
    // this can happen because no value was set
    // e.g. banned.users=#this is a comment
    int has_values = 1;
    if (strstr(line, splitter) == NULL) {
      fprintf(ERRORFILE, "configuration tokenization failed, error with line %s\n", line);
      has_values = 0;
    }

    // It is not a valid line, free memory.
    free((void *) section->kv_pairs[section->size]->key);
    free((void *) section->kv_pairs[section->size]);
    section->kv_pairs[section->size] = NULL;
    free(buffer);

    // Return -1 when no values
    if (!has_values) {
      return -1;
    }

    // Return 2 for comments
    return 2;
  }

#ifdef DEBUG
  fprintf(LOGFILE, "read_config : Adding conf value : %s \n", equaltok);
#endif

  section->kv_pairs[section->size]->value = trim(equaltok);
  section->size++;
  free(buffer);
  return 0;
}

/**
 * Remove any trailing comment from the supplied line. Function modifies the
 * argument provided.
 *
 * @param line the line from which to remove the comment
 */
static void trim_comment(char *line) {
  char *begin_comment = NULL;
  if (line != NULL) {
    begin_comment = strchr(line, COMMENT_BEGIN_CHAR);
    if (begin_comment != NULL) {
      *begin_comment = '\0';
    }
  }
}

/**
 * Allocate a section struct and initialize it. The memory must be freed by
 * the caller. Function calls exit if any error occurs.
 *
 * @return pointer to the allocated section struct
 *
 */
static struct section *allocate_section() {
  struct section *section = (struct section *) malloc(sizeof(struct section));
  if (section == NULL) {
    fprintf(ERRORFILE, "malloc failed while allocating section.\n");
    exit(OUT_OF_MEMORY);
  }
  section->name = NULL;
  section->kv_pairs = NULL;
  section->size = 0;
  return section;
}

/**
 * Populate the given section struct with fields from the config file.
 *
 * @param conf_file the file to read from
 * @param section pointer to the section struct to populate
 *
 */
static void populate_section_fields(FILE *conf_file, struct section *section) {
  char *line;
  long int offset = 0;
  while (!feof(conf_file)) {
    offset = ftell(conf_file);
    line = read_config_line(conf_file);
    if (line != NULL) {
      if (!is_comment_line(line)) {
        trim_comment(line);
        if (!is_section_start_line(line)) {
          if (section->name != NULL) {
            if (read_section_entry(line, section) < 0) {
              fprintf(ERRORFILE, "Error parsing line %s", line);
              exit(INVALID_CONFIG_FILE);
            }
          } else {
            fprintf(ERRORFILE, "Line '%s' doesn't belong to a section\n",
                    line);
            exit(INVALID_CONFIG_FILE);
          }
        } else {
          if (section->name == NULL) {
            section->name = get_section_name(line);
            if (strlen(section->name) == 0) {
              fprintf(ERRORFILE, "Empty section name");
              exit(INVALID_CONFIG_FILE);
            }
          } else {
            // we've reached the next section
            fseek(conf_file, offset, SEEK_SET);
            free(line);
            return;
          }
        }
      }
      free(line);
    }
  }
}

/**
 * Read the section current section from the conf file. Section start is
 * marked by lines of the form '[section-name]' and continue till the next
 * section.
 */
static struct section *read_section(FILE *conf_file) {
  struct section *section = allocate_section();
  populate_section_fields(conf_file, section);
  if (section->name == NULL) {
    free_section(section);
    section = NULL;
  }
  return section;
}

/**
 * Merge two sections and free the second one after the merge, if desired.
 * @param section1 the first section
 * @param section2 the second section
 * @param free_second_section free the second section if set
 */
static void merge_sections(struct section *section1, struct section *section2, const int free_second_section) {
  int i = 0;
  section1->kv_pairs = (struct kv_pair **) realloc(
            section1->kv_pairs,
            sizeof(struct kv_pair *) * (section1->size + section2->size));
  if (section1->kv_pairs == NULL) {
    fprintf(ERRORFILE,
                "Failed re-allocating memory for configuration items\n");
    exit(OUT_OF_MEMORY);
  }
  for (i = 0; i < section2->size; ++i) {
    section1->kv_pairs[section1->size + i] = section2->kv_pairs[i];
  }
  section1->size += section2->size;
  if (free_second_section) {
    free(section2->name);
    memset(section2, 0, sizeof(*section2));
    free(section2);
  }
}

int read_config(const char *file_path, struct configuration *cfg) {
  FILE *conf_file;

  if (file_path == NULL) {
    fprintf(ERRORFILE, "Null configuration filename passed in\n");
    return INVALID_CONFIG_FILE;
  }

#ifdef DEBUG
  fprintf(LOGFILE, "read_config :Conf file name is : %s \n", file_path);
#endif

  cfg->size = 0;
  conf_file = fopen(file_path, "r");
  if (conf_file == NULL) {
    fprintf(ERRORFILE, "Invalid conf file provided, unable to open file"
        " : %s \n", file_path);
    return (INVALID_CONFIG_FILE);
  }

  cfg->sections = (struct section **) malloc(
        sizeof(struct section *) * MAX_SIZE);
  if (!cfg->sections) {
    fprintf(ERRORFILE,
            "Failed to allocate memory for configuration sections\n");
    exit(OUT_OF_MEMORY);
  }

  // populate any entries in the older format(no sections)
  cfg->sections[cfg->size] = allocate_section();
  cfg->sections[cfg->size]->name = strdup("");
  populate_section_fields(conf_file, cfg->sections[cfg->size]);
  if (cfg->sections[cfg->size]) {
    if (cfg->sections[cfg->size]->size) {
      cfg->size++;
    } else {
      free_section(cfg->sections[cfg->size]);
    }
  }

  // populate entries in the sections format
  while (!feof(conf_file)) {
    cfg->sections[cfg->size] = NULL;
    struct section *new_section = read_section(conf_file);
    if (new_section != NULL) {
      struct section *existing_section =
          get_configuration_section(new_section->name, cfg);
      if (existing_section != NULL) {
        merge_sections((struct section *) existing_section, new_section, 1);
      } else {
        cfg->sections[cfg->size] = new_section;
      }
    }

    // Check if we need to expand memory for sections.
    if (cfg->sections[cfg->size]) {
      if ((cfg->size + 1) % MAX_SIZE == 0) {
        cfg->sections = (struct section **) realloc(cfg->sections,
                           sizeof(struct sections *) * (MAX_SIZE + cfg->size));
        if (cfg->sections == NULL) {
          fprintf(ERRORFILE,
                  "Failed re-allocating memory for configuration items\n");
          exit(OUT_OF_MEMORY);
        }
      }
      cfg->size++;
    }
  }

  fclose(conf_file);

  if (cfg->size == 0) {
    free_configuration(cfg);
    fprintf(ERRORFILE, "Invalid configuration provided in %s\n", file_path);
    return INVALID_CONFIG_FILE;
  }
  return 0;
}

/*
 * function used to get a configuration value.
 * The function for the first time populates the configuration details into
 * array, next time onwards used the populated array.
 *
 */
char *get_section_value(const char *key, const struct section *section) {
  int count;
  if (key == NULL || section == NULL) {
    return NULL;
  }
  for (count = 0; count < section->size; count++) {
    if (strcmp(section->kv_pairs[count]->key, key) == 0) {
      return strdup(section->kv_pairs[count]->value);
    }
  }
  return NULL;
}

/**
 * Function to return an array of values for a key.
 * Value delimiter is assumed to be a ','.
 */
char **get_section_values(const char *key, const struct section *cfg) {
  return get_section_values_delimiter(key, cfg, ",");
}

/**
 * Function to return an array of values for a key, using the specified
 delimiter.
 */
char **get_section_values_delimiter(const char *key, const struct section *cfg,
                                    const char *delim) {
  if (key == NULL || cfg == NULL || delim == NULL) {
    return NULL;
  }
  char *value = get_section_value(key, cfg);
  char **split_values = split_delimiter(value, delim);

  if (value) {
    free(value);
  }

  return split_values;
}

char *get_configuration_value(const char *key, const char *section,
                              const struct configuration *cfg) {
  const struct section *section_ptr;
  if (key == NULL || section == NULL || cfg == NULL) {
    return NULL;
  }
  section_ptr = get_configuration_section(section, cfg);
  if (section_ptr != NULL) {
    return get_section_value(key, section_ptr);
  }
  return NULL;
}

char **get_configuration_values(const char *key, const char *section,
                                const struct configuration *cfg) {
  const struct section *section_ptr;
  if (key == NULL || section == NULL || cfg == NULL) {
    return NULL;
  }
  section_ptr = get_configuration_section(section, cfg);
  if (section_ptr != NULL) {
    return get_section_values(key, section_ptr);
  }
  return NULL;
}

char **get_configuration_values_delimiter(const char *key, const char *section,
                                          const struct configuration *cfg, const char *delim) {
  const struct section *section_ptr;
  if (key == NULL || section == NULL || cfg == NULL || delim == NULL) {
    return NULL;
  }
  section_ptr = get_configuration_section(section, cfg);
  if (section_ptr != NULL) {
    return get_section_values_delimiter(key, section_ptr, delim);
  }
  return NULL;
}

struct section *get_configuration_section(const char *section,
                                          const struct configuration *cfg) {
  int i = 0;
  if (cfg == NULL || section == NULL) {
    return NULL;
  }
  for (i = 0; i < cfg->size; ++i) {
    if (strcmp(cfg->sections[i]->name, section) == 0) {
      return cfg->sections[i];
    }
  }
  return NULL;
}

/**
 * If str is a string of the form key=val, find 'key'
 */
int get_kv_key(const char *input, char *out, size_t out_len) {

  if (input == NULL)
    return -EINVAL;

  const char *split = strchr(input, '=');

  if (split == NULL)
    return -EINVAL;

  unsigned long key_len = split - input;

  if (out_len < (key_len + 1) || out == NULL)
    return -ENAMETOOLONG;

  memcpy(out, input, key_len);
  out[key_len] = '\0';

  return 0;
}

/**
 * If str is a string of the form key=val, find 'val'
 */
int get_kv_value(const char *input, char *out, size_t out_len) {

  if (input == NULL)
    return -EINVAL;

  const char *split = strchr(input, '=');

  if (split == NULL)
    return -EINVAL;

  split++; // advance past '=' to the value
  unsigned long val_len = (input + strlen(input)) - split;

  if (out_len < (val_len + 1) || out == NULL)
    return -ENAMETOOLONG;

  memcpy(out, split, val_len);
  out[val_len] = '\0';

  return 0;
}

char *get_config_path(const char *argv0) {
  char *executable_file = get_executable((char *) argv0);
  if (!executable_file) {
    fprintf(ERRORFILE, "realpath of executable: %s\n",
            errno != 0 ? strerror(errno) : "unknown");
    return NULL;
  }

  const char *orig_conf_file = HADOOP_CONF_DIR "/" CONF_FILENAME;
  char *conf_file = resolve_config_path(orig_conf_file, executable_file);
  if (conf_file == NULL) {
    fprintf(ERRORFILE, "Configuration file %s not found.\n", orig_conf_file);
  }
  return conf_file;
}
