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
#include <linux/loop.h>
#include <sys/types.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include "../modules/common/module-configs.h"
// TODO: Figure out how to address new openssl dependency for container-executor
#include <openssl/evp.h>

// workaround for building on RHEL6 but running on RHEL7
#ifndef LOOP_CTL_GET_FREE
#define LOOP_CTL_GET_FREE  0x4C82
#endif

#include "utils/string-utils.h"
#include "util.h"
#include "configuration.h"
#include "container-executor.h"

#include "runc.h"
#include "runc_base_ctx.h"
#include "runc_config.h"
#include "runc_launch_cmd.h"
#include "runc_reap.h"
#include "runc_write_config.h"

#define NUM_ROOTFS_UNMOUNT_ATTEMPTS      40
#define MAX_ROOTFS_UNMOUNT_BACKOFF_MSEC  1000

// NOTE: Update init_runc_overlay_desc and destroy_runc_overlay_desc
//       when this is changed.
typedef struct runc_overlay_desc_struct {
  char* top_path;             // top-level directory
  char* mount_path;           // overlay mount point under top_path
  char* upper_path;           // root path of upper layer under top_path
  char* work_path;            // overlay work path under top_path
} runc_overlay_desc;

// NOTE: Update init_runc_mount_context and destroy_runc_mount_context
//       when this is changed.
typedef struct runc_mount_context_struct {
  char* src_path;             // path to raw layer data
  char* layer_path;           // path under layer database for this layer
  char* mount_path;           // mount point of filesystem under layer_path
  int fd;                     // opened file descriptor or -1
} runc_mount_ctx;

// NOTE: Update init_runc_launch_cmd_ctx and destroy_runc_launch_cmd_ctx
//       when this is changed.
typedef struct runc_launch_cmd_context_struct {
  runc_base_ctx base_ctx;      // run root and layer lock
  runc_overlay_desc upper;     // writable upper layer descriptor
  runc_mount_ctx* layers;      // layer mount info
  unsigned int num_layers;    // number of layer mount contexts
} runc_launch_cmd_ctx;

int runc_module_enabled(const struct configuration *conf) {
  struct section *section = get_configuration_section(CONTAINER_EXECUTOR_CFG_RUNC_SECTION, conf);
  if (section != NULL) {
    return module_enabled(section, CONTAINER_EXECUTOR_CFG_RUNC_SECTION);
  }
  return 0;
}

static void init_runc_overlay_desc(runc_overlay_desc* desc) {
  memset(desc, 0, sizeof(*desc));
}

static void destroy_runc_overlay_desc(runc_overlay_desc* desc) {
  if (desc != NULL) {
    free(desc->top_path);
    free(desc->mount_path);
    free(desc->upper_path);
    free(desc->work_path);
  }
}

static void init_runc_mount_ctx(runc_mount_ctx* ctx) {
  memset(ctx, 0, sizeof(*ctx));
  ctx->fd = -1;
}

static void destroy_runc_mount_ctx(runc_mount_ctx* ctx) {
  if (ctx != NULL) {
    free(ctx->src_path);
    free(ctx->layer_path);
    free(ctx->mount_path);
    if (ctx->fd != -1) {
      close(ctx->fd);
    }
  }
}

static void init_runc_launch_cmd_ctx(runc_launch_cmd_ctx* ctx) {
  memset(ctx, 0, sizeof(*ctx));
  init_runc_base_ctx(&ctx->base_ctx);
  init_runc_overlay_desc(&ctx->upper);
}

static void destroy_runc_launch_cmd_ctx(runc_launch_cmd_ctx* ctx) {
  if (ctx != NULL) {
    if (ctx->layers != NULL) {
      for (unsigned int i = 0; i < ctx->num_layers; ++i) {
        destroy_runc_mount_ctx(&ctx->layers[i]);
      }
      free(ctx->layers);
    }
    destroy_runc_overlay_desc(&ctx->upper);
    destroy_runc_base_ctx(&ctx->base_ctx);
  }
}

static runc_launch_cmd_ctx* alloc_runc_launch_cmd_ctx() {
  runc_launch_cmd_ctx* ctx = malloc(sizeof(*ctx));
  if (ctx != NULL) {
    init_runc_launch_cmd_ctx(ctx);
  }
  return ctx;
}

static void free_runc_launch_cmd_ctx(runc_launch_cmd_ctx* ctx) {
  if (ctx != NULL) {
    destroy_runc_launch_cmd_ctx(ctx);
    free(ctx);
  }
}

static runc_launch_cmd_ctx* setup_runc_launch_cmd_ctx() {
  runc_launch_cmd_ctx* ctx = alloc_runc_launch_cmd_ctx();
  if (ctx == NULL) {
    fputs("Cannot allocate memory\n", ERRORFILE);
    return NULL;
  }

  if (!open_runc_base_ctx(&ctx->base_ctx)) {
    free_runc_launch_cmd_ctx(ctx);
    return NULL;
  }

  return ctx;
}

/**
 * Compute a digest of a layer based on the layer's pathname.
 * Returns the malloc'd digest hexstring or NULL if there was an error.
 */
static char* compute_layer_hash(const char* path) {
  char* digest = NULL;
  EVP_MD_CTX* mdctx = EVP_MD_CTX_create();
  if (mdctx == NULL) {
    fputs("Unable to create EVP MD context\n", ERRORFILE);
    goto cleanup;
  }

  if (!EVP_DigestInit_ex(mdctx, EVP_sha256(), NULL)) {
    fputs("Unable to initialize SHA256 digester\n", ERRORFILE);
    goto cleanup;
  }

  if (!EVP_DigestUpdate(mdctx, path, strlen(path))) {
    fputs("Unable to compute layer path digest\n", ERRORFILE);
    goto cleanup;
  }

  unsigned char raw_digest[EVP_MAX_MD_SIZE];
  unsigned int raw_digest_len = 0;
  if (!EVP_DigestFinal_ex(mdctx, raw_digest, &raw_digest_len)) {
    fputs("Unable to compute layer path digest\n", ERRORFILE);
    goto cleanup;
  }

  digest = to_hexstring(raw_digest, raw_digest_len);

cleanup:
  if (mdctx != NULL) {
    EVP_MD_CTX_destroy(mdctx);
  }
  return digest;
}

/**
 * Open the specified path which is expected to be a mount point.
 *
 * Returns an valid file descriptor when the path exists and is a mount point
 * or -1 if the path does not exist or is not a mount point.
 *
 * NOTE: The corresponding read lock must be acquired.
 */
static int open_mountpoint(const char* path) {
  int fd = open(path, O_RDONLY | O_CLOEXEC);
  if (fd == -1) {
    if (errno != ENOENT) {
      fprintf(ERRORFILE, "Error accessing mount point at %s : %s\n", path,
          strerror(errno));
    }
    return fd;
  }

  struct stat mstat, pstat;
  if (fstat(fd, &mstat) == -1) {
    fprintf(ERRORFILE, "Error accessing mount point at %s : %s\n", path,
        strerror(errno));
    goto close_fail;
  }
  if (!S_ISDIR(mstat.st_mode)) {
    fprintf(ERRORFILE, "Mount point %s is not a directory\n", path);
    goto close_fail;
  }

  if (fstatat(fd, "..", &pstat, 0) == -1) {
    fprintf(ERRORFILE, "Error accessing mount point parent of %s : %s\n", path,
        strerror(errno));
    goto close_fail;
  }

  // If the parent directory's device matches the child directory's device
  // then we didn't cross a device boundary in the filesystem and therefore
  // this is likely not a mount point.
  // TODO: This assumption works for loopback mounts but would not work for
  //       bind mounts or some other situations. Worst case would need to
  //       walk the mount table and otherwise replicate the mountpoint(1) cmd.
  if (mstat.st_dev == pstat.st_dev) {
    goto close_fail;
  }

  return fd;

close_fail:
  close(fd);
  return -1;
}

static bool init_overlay_descriptor(runc_overlay_desc* desc,
    const char* run_root, const char* container_id) {
  if (asprintf(&desc->top_path, "%s/%s", run_root, container_id) == -1) {
    return false;
  }
  if (asprintf(&desc->mount_path, "%s/rootfs", desc->top_path) == -1) {
    return false;
  }
  if (asprintf(&desc->upper_path, "%s/upper", desc->top_path) == -1) {
    return false;
  }
  if (asprintf(&desc->work_path, "%s/work", desc->top_path) == -1) {
    return false;
  }
  return true;
}

static bool init_layer_mount_ctx(runc_mount_ctx* ctx, const rlc_layer_spec* spec,
    const char* run_root) {
  char* hash = compute_layer_hash(spec->path);
  if (hash == NULL) {
    return false;
  }

  ctx->layer_path = get_runc_layer_path(run_root, hash);
  free(hash);
  if (ctx->layer_path == NULL) {
    return false;
  }

  ctx->mount_path = get_runc_layer_mount_path(ctx->layer_path);
  if (ctx->mount_path == NULL) {
    return false;
  }

  ctx->fd = open(spec->path, O_RDONLY | O_CLOEXEC);
  if (ctx->fd == -1) {
    fprintf(ERRORFILE, "Error opening layer image at %s : %s\n", spec->path,
        strerror(errno));
    return false;
  }

  ctx->src_path = strdup(spec->path);
  return ctx->src_path != NULL;
}

/**
 * Initialize the layers mount contexts and open each layer image as the user
 * to validate the user should be allowed to access the image composed of
 * these layers.
 */
static bool init_layer_mount_ctxs(runc_launch_cmd_ctx* ctx,
    const rlc_layer_spec* layer_specs, unsigned int num_layers) {
  ctx->layers = malloc(num_layers * sizeof(*ctx->layers));
  if (ctx->layers == NULL) {
    fputs("Unable to allocate memory\n", ERRORFILE);
    return false;
  }

  for (unsigned int i = 0; i < num_layers; ++i) {
    init_runc_mount_ctx(&ctx->layers[i]);
  }
  ctx->num_layers = num_layers;

  for (unsigned int i = 0; i < num_layers; ++i) {
    if (!init_layer_mount_ctx(&ctx->layers[i], &layer_specs[i],
        ctx->base_ctx.run_root)) {
      return false;
    }
  }

  return true;
}

/**
 * Allocate a loop device and assruncate it with a file descriptor.
 * Returns the file descriptor of the opened loop device or -1 on error.
 */
static int allocate_and_open_loop_device(char** loopdev_name_out, int src_fd) {
  *loopdev_name_out = NULL;
  int loopctl = open("/dev/loop-control", O_RDWR);
  if (loopctl == -1) {
    fprintf(ERRORFILE, "Error opening /dev/loop-control : %s\n",
        strerror(errno));
    return -1;
  }

  char* loopdev_name = NULL;
  int loop_fd = -1;
  while (true) {
    int loop_num = ioctl(loopctl, LOOP_CTL_GET_FREE);
    if (loop_num < 0) {
      fprintf(ERRORFILE, "Error allocating a new loop device: %s\n",
          strerror(errno));
      goto fail;
    }

    if (asprintf(&loopdev_name, "/dev/loop%d", loop_num) == -1) {
      fputs("Unable to allocate memory\n", ERRORFILE);
      goto fail;
    }
    loop_fd = open(loopdev_name, O_RDWR | O_CLOEXEC);
    if (loop_fd == -1) {
      fprintf(ERRORFILE, "Unable to open loop device at %s : %s\n",
          loopdev_name, strerror(errno));
      goto fail;
    }

    if (ioctl(loop_fd, LOOP_SET_FD, src_fd) != -1) {
      break;
    }

    // EBUSY indicates another process stole this loop device
    if (errno != EBUSY) {
      fprintf(ERRORFILE, "Error setting loop source file: %s\n",
          strerror(errno));
      goto fail;
    }

    close(loop_fd);
    loop_fd = -1;
    free(loopdev_name);
    loopdev_name = NULL;
  }

  struct loop_info64 loop_info;
  memset(&loop_info, 0, sizeof(loop_info));
  loop_info.lo_flags = LO_FLAGS_READ_ONLY | LO_FLAGS_AUTOCLEAR;
  if (ioctl(loop_fd, LOOP_SET_STATUS64, &loop_info) == -1) {
    fprintf(ERRORFILE, "Error setting loop flags: %s\n", strerror(errno));
    goto fail;
  }

  close(loopctl);
  *loopdev_name_out = loopdev_name;
  return loop_fd;

fail:
  if (loop_fd != -1) {
    close(loop_fd);
  }
  close(loopctl);
  free(loopdev_name);
  return -1;
}

/**
 * Mount a filesystem with the specified arguments, see the mount(2) manpage.
 * If the mount fails an error message is printed to ERRORFILE.
 * Returns true for success or false on failure.
 */
static bool do_mount(const char* src, const char* target,
    const char* fs_type, unsigned long mount_flags, const char* mount_options) {
  if (mount(src, target, fs_type, mount_flags, mount_options) == -1) {
    const char* nullstr = "NULL";
    src = (src != NULL) ? src : nullstr;
    fs_type = (fs_type != NULL) ? fs_type : nullstr;
    mount_options = (mount_options != NULL) ? mount_options : nullstr;
    fprintf(ERRORFILE, "Error mounting %s at %s type %s with options %s : %s\n",
        src, target, fs_type, mount_options, strerror(errno));
    return false;
  }
  return true;
}

/**
 * Mount a filesystem and return a file descriptor opened to the mount point.
 * The mount point directory will be created if necessary.
 * Returns a file descriptor to the mount point or -1 if there was an error.
 */
static int mount_and_open(const char* src, const char* target,
    const char* fs_type, unsigned long mount_flags, const char* mount_options) {
  if (mkdir(target, S_IRWXU) == -1 && errno != EEXIST) {
    fprintf(ERRORFILE, "Error creating mountpoint directory at %s : %s\n",
        target, strerror(errno));
    return -1;
  }

  if (!do_mount(src, target, fs_type, mount_flags, mount_options)) {
    return -1;
  }

  return open_mountpoint(target);
}

static int mount_layer_and_open(const runc_mount_ctx* layer) {
  if (mkdir(layer->layer_path, S_IRWXU) == -1) {
    if (errno != EEXIST) {
      fprintf(ERRORFILE, "Error creating layer directory at %s : %s\n",
          layer->layer_path, strerror(errno));
      return -1;
    }
  }

  char *loopdev_name = NULL;
  int loopfd = allocate_and_open_loop_device(&loopdev_name, layer->fd);
  if (loopfd == -1) {
    return -1;
  }

  int mount_fd = mount_and_open(loopdev_name, layer->mount_path, "squashfs",
    MS_RDONLY, NULL);

  // If the mount worked then the mount holds the loop device open. If the mount
  // failed then the loop device is no longer needed, so close it either way.
  close(loopfd);

  free(loopdev_name);
  return mount_fd;
}

static bool do_mount_layers_with_lock(runc_launch_cmd_ctx* ctx) {
  bool have_write_lock = false;
  for (unsigned int i = 0; i < ctx->num_layers; ++i) {
    int layer_mount_fd = open_mountpoint(ctx->layers[i].mount_path);
    if (layer_mount_fd != -1) {
      // Touch layer directory to show this existing layer was recently used.
      if (utimes(ctx->layers[i].layer_path, NULL) == -1) {
        // Error is not critical to container launch so just print a warning.
        fprintf(ERRORFILE, "Error updating timestamps of %s : %s\n",
            ctx->layers[i].layer_path, strerror(errno));
      }
    } else {
      if (!have_write_lock) {
        if (!acquire_runc_layers_write_lock(&ctx->base_ctx)) {
          return false;
        }
        have_write_lock = true;
        // Try to open the mount point again in case another process created it
        // while we were waiting for the write lock.
        layer_mount_fd = open_mountpoint(ctx->layers[i].mount_path);
      }
      if (layer_mount_fd == -1) {
        layer_mount_fd = mount_layer_and_open(&ctx->layers[i]);

        if (layer_mount_fd == -1) {
          fprintf(ERRORFILE, "Unable to mount layer data from %s\n",
              ctx->layers[i].src_path);
          return false;
        }
      }
    }

    // Now that the layer is mounted we can start tracking the open mount point
    // for the layer rather than the descriptor to the layer image.
    // The mount point references the underlying image, so we no longer need
    // a direct reference to the layer image.
    close(ctx->layers[i].fd);
    ctx->layers[i].fd = layer_mount_fd;
  }

  return true;
}

static bool mount_layers(runc_launch_cmd_ctx* ctx) {
  if (!acquire_runc_layers_read_lock(&ctx->base_ctx)) {
    return false;
  }

  bool result = do_mount_layers_with_lock(ctx);

  if (!release_runc_layers_lock(&ctx->base_ctx)) {
    return false;
  }

  return result;
}

static char* build_overlay_options(runc_mount_ctx* layers,
    unsigned int num_layers, const runc_overlay_desc* upper) {
  char* result = NULL;
  const int sb_incr = 16*1024;
  strbuf sb;
  if (!strbuf_init(&sb, sb_incr)) {
    fputs("Unable to allocate memory\n", ERRORFILE);
    goto cleanup;
  }

  if (!strbuf_append_fmt(&sb, sb_incr, "upperdir=%s,workdir=%s,lowerdir=",
      upper->upper_path, upper->work_path)) {
    goto cleanup;
  }

  // Overlay expects the base layer to be the last layer listed, but the
  // OCI image manifest specifies the base layer first.
  bool need_separator = false;
  for (int i = num_layers - 1; i >= 0; --i) {
    char* fmt = need_separator ? ":%s" : "%s";
    if (!strbuf_append_fmt(&sb, sb_incr, fmt, layers[i].mount_path)) {
      goto cleanup;
    }
    need_separator = true;
  }

  result = strbuf_detach_buffer(&sb);

cleanup:
  strbuf_destroy(&sb);
  return result;
}

static bool create_overlay_dirs(runc_overlay_desc* od) {
  if (mkdir(od->top_path, S_IRWXU) != 0) {
    fprintf(ERRORFILE, "Error creating %s : %s\n", od->top_path,
        strerror(errno));
    return false;
  }

  if (mkdir(od->mount_path, S_IRWXU) != 0) {
    fprintf(ERRORFILE, "Error creating %s : %s\n", od->mount_path,
        strerror(errno));
    return false;
  }

  mode_t upper_mode = S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;
  if (mkdir(od->upper_path, upper_mode) != 0) {
    fprintf(ERRORFILE, "Error creating %s : %s\n", od->upper_path,
        strerror(errno));
    return false;
  }

  if (mkdir(od->work_path, S_IRWXU) != 0) {
    fprintf(ERRORFILE, "Error creating %s : %s\n", od->work_path,
        strerror(errno));
    return false;
  }

  return true;
}

static bool mount_container_rootfs(runc_launch_cmd_ctx* ctx) {
  if (!create_overlay_dirs(&ctx->upper)) {
    return false;
  }

  if (!mount_layers(ctx)) {
    return false;
  }

  char* overlay_opts = build_overlay_options(ctx->layers, ctx->num_layers,
      &ctx->upper);
  if (overlay_opts == NULL) {
    return false;
  }

  bool mount_ok = do_mount("overlay", ctx->upper.mount_path, "overlay", 0,
      overlay_opts);
  free(overlay_opts);
  if (!mount_ok) {
    return false;
  }

  // It would be tempting to close the layer file descriptors here since the
  // overlay should also be holding references to all the layers.  However
  // overlay somehow does NOT hold a hard reference to underlying filesystems,
  // so the layer file descriptors need to be kept open in order to prevent
  // other containers from unmounting shared layers when they cleanup.

  return true;
}

static bool unmount_and_remove(const char* path) {
  if (umount(path) == -1 && errno != EINVAL && errno != ENOENT) {
    if (errno == EBUSY) {
      // Layer is in use by another container.
      return false;
    }
    fprintf(ERRORFILE, "Error unmounting %s : %s\n", path, strerror(errno));
    return false;
  }
  if (rmdir(path) == -1 && errno != ENOENT) {
    fprintf(ERRORFILE, "Error removing mount directory %s : %s\n", path,
        strerror(errno));
    return false;
  }
  return true;
}

static bool unmount_and_remove_with_retry(const char* path, int max_attempts,
    long max_backoff_msec) {
  long backoff_msec = 1;
  for (int i = 0; i < max_attempts - 1; ++i) {
    if (unmount_and_remove(path)) {
      return true;
    }
    struct timespec ts;
    memset(&ts, 0, sizeof(ts));
    ts.tv_sec = backoff_msec / 1000;
    ts.tv_nsec = (backoff_msec % 1000) * 1000 * 1000;
    nanosleep(&ts, NULL);
    backoff_msec *= 2;
    if (backoff_msec > max_backoff_msec) {
      backoff_msec = max_backoff_msec;
    }
  }

  return unmount_and_remove(path);
}

static bool rmdir_recursive_fd(int fd) {
  int dirfd = dup(fd);
  if (dirfd == -1) {
    fputs("Unable to duplicate file descriptor\n", ERRORFILE);
    return false;
  }

  DIR* dir = fdopendir(dirfd);
  if (dir == NULL) {
    fprintf(ERRORFILE, "Error deleting directory: %s\n", strerror(errno));
    return false;
  }

  bool result = false;
  struct dirent* de;
  while ((de = readdir(dir)) != NULL) {
    if (strcmp(".", de->d_name) == 0 || strcmp("..", de->d_name) == 0) {
      continue;
    }

    struct stat statbuf;
    if (fstatat(dirfd, de->d_name, &statbuf, AT_SYMLINK_NOFOLLOW) == -1) {
      if (errno == ENOENT) {
        continue;
      }
      fprintf(ERRORFILE, "Error accessing %s : %s\n", de->d_name,
          strerror(errno));
      goto cleanup;
    }

    int rmflags = 0;
    if (S_ISDIR(statbuf.st_mode)) {
      rmflags = AT_REMOVEDIR;
      int de_fd = openat(dirfd, de->d_name, O_RDONLY | O_NOFOLLOW);
      if (de_fd == -1) {
        if (errno == ENOENT) {
          continue;
        }
        fprintf(ERRORFILE, "Error opening %s for delete: %s\n", de->d_name,
            strerror(errno));
        goto cleanup;
      }
      bool ok = rmdir_recursive_fd(de_fd);
      close(de_fd);
      if (!ok) {
        goto cleanup;
      }
    }

    if (unlinkat(dirfd, de->d_name, rmflags) == -1 && errno != ENOENT) {
      fprintf(ERRORFILE, "Error removing %s : %s\n", de->d_name,
          strerror(errno));
      goto cleanup;
    }
  }

  result = true;

cleanup:
  closedir(dir);
  return result;
}

static bool rmdir_recursive(const char* path) {
  int fd = open(path, O_RDONLY | O_NOFOLLOW);
  if (fd == -1) {
    if (errno == ENOENT) {
      return true;
    }
    fprintf(ERRORFILE, "Error opening %s for delete: %s\n", path,
        strerror(errno));
    return false;
  }
  bool result = rmdir_recursive_fd(fd);
  close(fd);
  if (rmdir(path) == -1) {
    fprintf(ERRORFILE, "Error deleting %s : %s\n", path, strerror(errno));
    result = false;
  }
  return result;
}

static void close_layer_fds(runc_launch_cmd_ctx* ctx) {
  for (unsigned int i = 0; i < ctx->num_layers; ++i) {
    if (ctx->layers[i].fd != -1) {
      close(ctx->layers[i].fd);
      ctx->layers[i].fd = -1;
    }
  }
}

/**
 * Unmounts the container rootfs directory and MAY unmount layers on the host
 * based on the specified number of total layer mounts on the host specified.
 */
static void cleanup_container_mounts(runc_launch_cmd_ctx* ctx,
    int num_reap_layers_keep) {
  unmount_and_remove_with_retry(ctx->upper.mount_path,
      NUM_ROOTFS_UNMOUNT_ATTEMPTS, MAX_ROOTFS_UNMOUNT_BACKOFF_MSEC);
  rmdir_recursive(ctx->upper.top_path);
  reap_runc_layer_mounts_with_ctx(&ctx->base_ctx, num_reap_layers_keep);
}

/**
 * Unmounts the container rootfs directory and MAY unmount layers on the host
 * based on the specified number of total layer mounts on the host specified.
 *
 * IMPORTANT NOTE: This method may perform the unmount in a background process
 *                 and can return before that has completed!
 */
static void background_cleanup_container_mounts(runc_launch_cmd_ctx* ctx,
    int num_reap_layers_keep) {
  pid_t child_pid = fork();
  if (child_pid == -1) {
    fprintf(ERRORFILE, "Error forking child process: %s\n", strerror(errno));
    // try to clean it up in the foreground process
    child_pid = 0;
  }
  if (child_pid == 0) {
    cleanup_container_mounts(ctx, num_reap_layers_keep);
  }
}

static void exec_runc(const char* container_id, const char* runc_config_path,
    const char* pid_file_path) {
  char* runc_path = get_configuration_value(RUNC_BINARY_KEY, CONTAINER_EXECUTOR_CFG_RUNC_SECTION, get_cfg());
  if (runc_path == NULL) {
    runc_path = strdup(DEFAULT_RUNC_BINARY);
    if (runc_path == NULL) {
      fputs("Unable to allocate memory\n", ERRORFILE);
      exit(1);
    }
  }

  char* dir_end = strrchr(runc_config_path, '/');
  if (dir_end == NULL) {
    fprintf(ERRORFILE, "Error getting bundle path from config path %s\n",
        runc_config_path);
    exit(1);
  }
  char* bundle_path = strndup(runc_config_path, dir_end - runc_config_path);

  const char* const runc_args[] = {
      runc_path, "run",
      "--pid-file", pid_file_path,
      "-b", bundle_path,
      container_id,
      NULL
  };
  const char* const runc_env[] = { NULL };

  if (execve(runc_path, (char* const*)runc_args, (char* const*)runc_env) == -1) {
    char* errstr = strerror(errno);
    fputs("Failed to exec:", ERRORFILE);
    for (const char* const* argp = runc_args; *argp != NULL; ++argp) {
      fprintf(ERRORFILE, " %s", *argp);
    }
    fprintf(ERRORFILE, " : %s\n", errstr);
  }

  exit(1);
}

int run_runc_container(const char* command_file) {
  int rc = 0;
  char* exit_code_file = NULL;
  char* runc_config_path = NULL;
  runc_launch_cmd* rlc = NULL;
  runc_launch_cmd_ctx* ctx = setup_runc_launch_cmd_ctx();
  if (ctx == NULL) {
    rc = ERROR_RUNC_SETUP_FAILED;
    goto cleanup;
  }

  rlc = parse_runc_launch_cmd(command_file);
  if (rlc == NULL) {
    rc = ERROR_RUNC_SETUP_FAILED;
    goto cleanup;
  }

  rc = set_user(rlc->run_as_user);
  if (rc != 0) {
    goto cleanup;
  }

  exit_code_file = get_exit_code_file(rlc->pid_file);
  if (exit_code_file == NULL) {
    rc = OUT_OF_MEMORY;
    goto cleanup;
  }

  const char* work_dir = rlc->config.process.cwd->valuestring;
  rc = setup_container_paths(rlc->username, rlc->app_id, rlc->container_id,
     work_dir, rlc->script_path, rlc->cred_path, rlc->https, rlc->keystore_path,
     rlc->truststore_path, rlc->local_dirs, rlc->log_dirs);
  if (rc != 0) {
    goto cleanup;
  }

  rc = ERROR_RUNC_RUN_FAILED;
  if (!is_valid_runc_launch_cmd(rlc)) {
    goto cleanup;
  }

  if (!init_layer_mount_ctxs(ctx, rlc->layers, rlc->num_layers)) {
    goto cleanup;
  }

  if (!init_overlay_descriptor(&ctx->upper, ctx->base_ctx.run_root,
      rlc->container_id)) {
    goto cleanup;
  }

  runc_config_path = write_runc_runc_config(rlc, ctx->upper.mount_path);
  if (runc_config_path == NULL) {
    goto cleanup;
  }

  if (seteuid(0) != 0) {
    fputs("Unable to become root\n", ERRORFILE);
    goto cleanup;
  }

  if (!mount_container_rootfs(ctx)) {
    goto umount_and_cleanup;
  }

  pid_t child_pid = fork();
  if (child_pid == 0) {
    exec_runc(rlc->container_id, runc_config_path, rlc->pid_file);
    _exit(1);  // just in case exec_runc returns somehow
  } else if (child_pid == -1) {
    fprintf(ERRORFILE, "Error cannot fork: %s\n", strerror(errno));
    rc = OUT_OF_MEMORY;
    goto umount_and_cleanup;
  }

  rc = wait_and_write_exit_code(child_pid, exit_code_file);

umount_and_cleanup:
  // Container is no longer running, so layer references are no longer desired.
  close_layer_fds(ctx);

  // Cleanup mounts in a background process to keep it off the critical path.
  background_cleanup_container_mounts(ctx, rlc->num_reap_layers_keep);

cleanup:
  free(exit_code_file);
  free(runc_config_path);
  free_runc_launch_cmd(rlc);
  free_runc_launch_cmd_ctx(ctx);
  return rc;
}
