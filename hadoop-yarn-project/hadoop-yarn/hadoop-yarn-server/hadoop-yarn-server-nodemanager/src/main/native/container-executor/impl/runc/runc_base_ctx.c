/*
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

#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "configuration.h"
#include "container-executor.h"
#include "util.h"

#include "runc_base_ctx.h"
#include "runc_config.h"

#define LAYER_MOUNT_SUFFIX      "/mnt"
#define LAYER_MOUNT_SUFFIX_LEN  (sizeof(LAYER_MOUNT_SUFFIX) -1)

/**
 * Get the path to the runtime layers directory.
 *
 * Returns the heap-allocated path to the layers directory or NULL on error.
 */
char* get_runc_layers_path(const char* run_root) {
  char* layers_path = NULL;
  if (asprintf(&layers_path, "%s/layers", run_root) == -1) {
    layers_path = NULL;
  }
  return layers_path;
}

/**
 * Get the path to a layer directory.
 *
 * Returns the heap-allocated path to the layer directory or NULL on error.
 */
char* get_runc_layer_path(const char* run_root, const char* layer_name) {
  char* layer_path = NULL;
  if (asprintf(&layer_path, "%s/layers/%s", run_root, layer_name) == -1) {
    layer_path = NULL;
  }
  return layer_path;
}

/**
 * Get the path to a layer's mountpoint.
 *
 * Returns the heap-allocated path to the layer's mountpoint or NULL on error.
 */
char* get_runc_layer_mount_path(const char* layer_path) {
  char* mount_path = NULL;
  if (asprintf(&mount_path, "%s" LAYER_MOUNT_SUFFIX, layer_path) == -1) {
    mount_path = NULL;
  }
  return mount_path;
}

/**
 * Get the layer path from a layer's mountpoint.
 *
 * Returns the heap-allocated path to the layer directory or NULL on error.
 */
char* get_runc_layer_path_from_mount_path(const char* mount_path) {
  size_t mount_path_len = strlen(mount_path);
  if (mount_path_len <= LAYER_MOUNT_SUFFIX_LEN) {
    return NULL;
  }
  size_t layer_path_len = mount_path_len - LAYER_MOUNT_SUFFIX_LEN;
  const char* suffix = mount_path + layer_path_len;
  if (strcmp(suffix, LAYER_MOUNT_SUFFIX)) {
    return NULL;
  }
  return strndup(mount_path, layer_path_len);
}

/**
 * Creates the run root directory and layers directory structure
 * underneath if necessary.
 * Returns the malloc'd run root path or NULL if there was an error.
 */
static char* setup_runc_run_root_directories() {
  char* layers_path = NULL;
  char* run_root = get_configuration_value(RUNC_RUN_ROOT_KEY,
      CONTAINER_EXECUTOR_CFG_RUNC_SECTION, get_cfg());
  if (run_root == NULL) {
    run_root = strdup(DEFAULT_RUNC_ROOT);
    if (run_root == NULL) {
      goto mem_fail;
    }
  }

  if (mkdir(run_root, S_IRWXU) != 0 && errno != EEXIST) {
    fprintf(ERRORFILE, "Error creating runC run root at %s : %s\n", run_root,
        strerror(errno));
    goto fail;
  }

  layers_path = get_runc_layers_path(run_root);
  if (layers_path == NULL) {
    goto mem_fail;
  }

  if (mkdir(layers_path, S_IRWXU) != 0 && errno != EEXIST) {
    fprintf(ERRORFILE, "Error creating layers directory at %s : %s\n",
        layers_path, strerror(errno));
    goto fail;
  }

  free(layers_path);
  return run_root;

fail:
  free(layers_path);
  free(run_root);
  return NULL;

mem_fail:
  fputs("Cannot allocate memory\n", ERRORFILE);
  goto fail;
}


/**
 * Initialize an uninitialized runC base context.
 */
void init_runc_base_ctx(runc_base_ctx* ctx) {
  memset(ctx, 0, sizeof(*ctx));
  ctx->layers_lock_fd = -1;
  ctx->layers_lock_state = F_UNLCK;
}

/**
 * Releases the resources underneath a runC base context but does NOT free the
 * structure itself. This is particularly useful for stack-allocated contexts
 * or structures that embed the context.
 * free_runc_base_ctx should be used for heap-allocated contexts.
 */
void destroy_runc_base_ctx(runc_base_ctx* ctx) {
  if (ctx != NULL) {
    free(ctx->run_root);
    if (ctx->layers_lock_fd != -1) {
      close(ctx->layers_lock_fd);
    }
  }
}

/**
 * Allocates and initializes a runC base context.
 *
 * Returns a pointer to the allocated and initialized context or NULL on error.
 */
runc_base_ctx* alloc_runc_base_ctx() {
  runc_base_ctx* ctx = malloc(sizeof(*ctx));
  if (ctx != NULL) {
    init_runc_base_ctx(ctx);
  }
  return ctx;
}

/**
 * Free a runC base context and all memory assruncated with it.
 */
void free_runc_base_ctx(runc_base_ctx* ctx) {
  destroy_runc_base_ctx(ctx);
  free(ctx);
}

/**
 * Opens the base context for use. This will create the container runtime
 * root directory and layer lock files, if necessary.
 *
 * Returns true on success or false if there was an error.
 */
bool open_runc_base_ctx(runc_base_ctx* ctx) {
  ctx->run_root = setup_runc_run_root_directories();
  if (ctx->run_root == NULL) {
    return false;
  }

  char* lock_path = get_runc_layer_path(ctx->run_root, "lock");
  if (lock_path == NULL) {
    fputs("Cannot allocate memory\n", ERRORFILE);
    return false;
  }

  bool result = true;
  ctx->layers_lock_fd = open(lock_path, O_RDWR | O_CREAT | O_CLOEXEC, S_IRWXU);
  if (ctx->layers_lock_fd == -1) {
    fprintf(ERRORFILE, "Cannot open lock file %s : %s\n", lock_path,
        strerror(errno));
    result = false;
  }

  free(lock_path);
  return result;
}

/**
 * Allocates and opens a base context.
 *
 * Returns a pointer to the context or NULL on error.
 */
runc_base_ctx* setup_runc_base_ctx() {
  runc_base_ctx* ctx = alloc_runc_base_ctx();
  if (ctx != NULL) {
    if (!open_runc_base_ctx(ctx)) {
      free_runc_base_ctx(ctx);
      ctx = NULL;
    }
  }
  return ctx;
}


static bool do_lock_cmd(int fd, int lock_cmd) {
  struct flock fl;
  memset(&fl, 0, sizeof(fl));
  fl.l_type = lock_cmd;
  fl.l_whence = SEEK_SET;
  fl.l_start = 0;
  fl.l_len = 0;
  while (true) {
    int rc = fcntl(fd, F_SETLKW, &fl);
    if (rc == 0) {
      return true;
    }
    if (errno != EINTR) {
      fprintf(ERRORFILE, "Error updating lock: %s\n", strerror(errno));
      return false;
    }
  }
}

/**
 * Acquire the layer read lock.
 *
 * Returns true on success or false on error.
 */
bool acquire_runc_layers_read_lock(runc_base_ctx* ctx) {
  if (ctx->layers_lock_state == F_RDLCK) {
    return true;
  }
  if (do_lock_cmd(ctx->layers_lock_fd, F_RDLCK)) {
    ctx->layers_lock_state = F_RDLCK;
    return true;
  }
  return false;
}

/**
 * Acquire the layer write lock.
 *
 * Returns true on success or false on error.
 */
bool acquire_runc_layers_write_lock(runc_base_ctx* ctx) {
  if (ctx->layers_lock_state == F_WRLCK) {
    return true;
  }
  if (ctx->layers_lock_state == F_RDLCK) {
    // Release before trying to acquire write lock, otherwise two processes
    // attempting to upgrade from read lock to a write lock can deadlock.
    if (!release_runc_layers_lock(ctx)) {
      return false;
    }
  }
  if (do_lock_cmd(ctx->layers_lock_fd, F_WRLCK)) {
    ctx->layers_lock_state = F_WRLCK;
    return true;
  }
  return false;
}

/**
 * Release the layer lock.
 *
 * Returns true on success or false on error.
 */
bool release_runc_layers_lock(runc_base_ctx* ctx) {
  if (ctx->layers_lock_state == F_UNLCK) {
    return true;
  }
  if (do_lock_cmd(ctx->layers_lock_fd, F_UNLCK)) {
    ctx->layers_lock_state = F_UNLCK;
    return true;
  }
  return false;
}
