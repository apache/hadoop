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

#define FILE_BUFFER_INCREMENT (128*1024)

#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "container-executor.h"
#include "file-utils.h"
#include "util.h"

/**
 * Read the contents of the specified file into an allocated buffer and return
 * the contents as a NUL-terminated string. NOTE: The file contents must not
 * contain a NUL character or the result will appear to be truncated.
 *
 * Returns a pointer to the allocated, NUL-terminated string or NULL on error.
 */
char* read_file_to_string(const char* filename) {
  char* buff = NULL;
  int rc = -1;
  int fd = open(filename, O_RDONLY);
  if (fd < 0) {
    fprintf(ERRORFILE, "Error opening %s : %s\n", filename, strerror(errno));
    goto cleanup;
  }

  struct stat filestat;
  if (fstat(fd, &filestat) != 0) {
    fprintf(ERRORFILE, "Error examining %s : %s\n", filename, strerror(errno));
    goto cleanup;
  }

  size_t buff_size = FILE_BUFFER_INCREMENT;
  if (S_ISREG(filestat.st_mode)) {
    buff_size = filestat.st_size + 1;  // +1 for terminating NUL
  }
  buff = malloc(buff_size);
  if (buff == NULL) {
    fprintf(ERRORFILE, "Unable to allocate %ld bytes\n", buff_size);
    goto cleanup;
  }

  int bytes_left = buff_size;
  char* cp = buff;
  int bytes_read;
  while ((bytes_read = read(fd, cp, bytes_left)) > 0) {
    cp += bytes_read;
    bytes_left -= bytes_read;
    if (bytes_left == 0) {
      buff_size += FILE_BUFFER_INCREMENT;
      bytes_left += FILE_BUFFER_INCREMENT;
      buff = realloc(buff, buff_size);
      if (buff == NULL) {
        fprintf(ERRORFILE, "Unable to allocate %ld bytes\n", buff_size);
        goto cleanup;
      }
    }
  }
  if (bytes_left < 0) {
    fprintf(ERRORFILE, "Error reading %s : %s\n", filename, strerror(errno));
    goto cleanup;
  }

  *cp = '\0';
  rc = 0;

cleanup:
  if (fd != -1) {
    close(fd);
  }
  if (rc != 0) {
    free(buff);
    buff = NULL;
  }
  return buff;
}

/**
 * Read a file to a string as the YARN nodemanager user and returns the
 * result as a string. See read_file_to_string for more details.
 *
 * Returns a pointer to the allocated, NUL-terminated string or NULL on error.
 */
char* read_file_to_string_as_nm_user(const char* filename) {
  uid_t user = geteuid();
  gid_t group = getegid();
  if (change_effective_user_to_nm() != 0) {
    fputs("Cannot change to nm user\n", ERRORFILE);
    return NULL;
  }

  char* buff = read_file_to_string(filename);
  if (change_effective_user(user, group) != 0) {
    fputs("Cannot revert to previous user\n", ERRORFILE);
    free(buff);
    return NULL;
  }
  return buff;
}

/**
 * Write a sequence of bytes to a new file as the YARN nodemanager user.
 *
 * Returns true on success or false on error.
 */
bool write_file_as_nm(const char* path, const void* data, size_t count) {
  bool result = false;
  int fd = -1;
  uid_t orig_user = geteuid();
  gid_t orig_group = getegid();
  if (change_effective_user_to_nm() != 0) {
    fputs("Error changing to NM user and group\n", ERRORFILE);
    return false;
  }

  fd = open(path, O_CREAT | O_EXCL | O_WRONLY, S_IRUSR | S_IWUSR);
  if (fd == -1) {
    fprintf(ERRORFILE, "Error creating %s : %s\n", path, strerror(errno));
    goto cleanup;
  }

  const uint8_t* bp = (const uint8_t*)data;
  while (count > 0) {
    ssize_t bytes_written = write(fd, bp, count);
    if (bytes_written == -1) {
      fprintf(ERRORFILE, "Error writing to %s : %s\n", path, strerror(errno));
      goto cleanup;
    }
    bp += bytes_written;
    count -= bytes_written;
  }

  result = true;

cleanup:
  if (fd != -1) {
    if (close(fd) == -1) {
      fprintf(ERRORFILE, "Error writing to %s : %s\n", path, strerror(errno));
      result = false;
    }
  }

  if (change_effective_user(orig_user, orig_group) != 0) {
    fputs("Cannot restore original user/group\n", ERRORFILE);
    result = false;
  }

  return result;
}
