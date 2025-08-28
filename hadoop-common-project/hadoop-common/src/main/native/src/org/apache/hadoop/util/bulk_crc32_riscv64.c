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

#include <assert.h>
#include <stddef.h>
#include <stdint.h>

#include "bulk_crc32.h"
#include "gcc_optimizations.h"

// CRC32C (iSCSI) polynomial constants
#define CRC32C_CONST_0     0xdd45aab8U
#define CRC32C_CONST_1     0x493c7d27U
#define CRC32C_CONST_QUO   0x0dea713f1ULL
#define CRC32C_CONST_POLY  0x105ec76f1ULL

// CRC32 (GZIP/zlib) polynomial constants
#define CRC32_CONST_0      0xb8bc6765U
#define CRC32_CONST_1      0xccaa009eU
#define CRC32_CONST_QUO    0x1f7011641ULL
#define CRC32_CONST_POLY   0x1db710641ULL

// Folding constants for CRC32C
static const uint64_t crc32c_fold_const[4] __attribute__((aligned(16))) = {
    0x00000000740eef02ULL, 0x000000009e4addf8ULL,
    0x00000000f20c0dfeULL, 0x00000000493c7d27ULL
};

// Folding constants for CRC32 (GZIP)
static const uint64_t crc32_fold_const[4] __attribute__((aligned(16))) = {
    0x000000008f352d95ULL, 0x000000001d9513d7ULL,
    0x00000000ae689191ULL, 0x00000000ccaa009eULL
};

/**
 * Hardware-accelerated CRC32C using RISC-V vector crypto extensions.
 * This uses the reflected polynomial version compatible with standard CRC32C.
 */
static uint32_t crc32c_vclmul(const uint8_t *buf, size_t len, uint32_t crc) {
    if (unlikely(len < 64)) {
        // Fall back to table-based implementation for small buffers
        return crc32c_sb8(crc, buf, len);
    }

    uint32_t result;
    const uint64_t *fold_consts = crc32c_fold_const;

    __asm__ __volatile__(
        // Initialize CRC
        "li             t5, 0xffffffff\n\t"
        "and            %[crc], %[crc], t5\n\t"
        "li             a3, 0\n\t"
        "li             t1, 64\n\t"

        // Set vector configuration for 128-bit elements
        "vsetivli       zero, 2, e64, m1, ta, ma\n\t"

        // Load first 64 bytes and initialize
        "mv             a4, %[buf]\n\t"
        "vle64.v        v0, 0(a4)\n\t"
        "addi           a4, a4, 16\n\t"
        "vle64.v        v1, 0(a4)\n\t"
        "addi           a4, a4, 16\n\t"
        "vle64.v        v2, 0(a4)\n\t"
        "addi           a4, a4, 16\n\t"
        "vle64.v        v3, 0(a4)\n\t"
        "addi           a4, a4, 16\n\t"
        "andi           a3, %[len], ~63\n\t"
        "addi           t0, a3, -64\n\t"

        // XOR initial CRC into first vector
        "vmv.s.x        v4, zero\n\t"
        "vmv.s.x        v5, %[crc]\n\t"
        "vslideup.vi    v5, v4, 1\n\t"
        "vxor.vv        v0, v0, v5\n\t"
        "vmv.s.x        v8, zero\n\t"

        // Load folding constant
        "add            a5, a4, t0\n\t"
        "mv             t4, %[consts]\n\t"
        "vle64.v        v5, 0(t4)\n\t"

        // Check if we need main loop
        "addi           t0, %[len], -64\n\t"
        "bltu           t0, t1, 2f\n\t"

        // Main loop - process 64 bytes at a time
        "1:\n\t"
        "vle64.v        v7, 0(a4)\n\t"
        "vclmul.vv      v4, v0, v5\n\t"
        "vclmulh.vv     v0, v0, v5\n\t"
        "vredxor.vs     v0, v0, v8\n\t"
        "vredxor.vs     v4, v4, v8\n\t"
        "vslideup.vi    v4, v0, 1\n\t"
        "vxor.vv        v0, v4, v7\n\t"

        "addi           a4, a4, 16\n\t"
        "vle64.v        v7, 0(a4)\n\t"
        "vclmul.vv      v4, v1, v5\n\t"
        "vclmulh.vv     v1, v1, v5\n\t"
        "vredxor.vs     v1, v1, v8\n\t"
        "vredxor.vs     v4, v4, v8\n\t"
        "vslideup.vi    v4, v1, 1\n\t"
        "vxor.vv        v1, v4, v7\n\t"

        "addi           a4, a4, 16\n\t"
        "vle64.v        v7, 0(a4)\n\t"
        "vclmul.vv      v4, v2, v5\n\t"
        "vclmulh.vv     v2, v2, v5\n\t"
        "vredxor.vs     v2, v2, v8\n\t"
        "vredxor.vs     v4, v4, v8\n\t"
        "vslideup.vi    v4, v2, 1\n\t"
        "vxor.vv        v2, v4, v7\n\t"

        "addi           a4, a4, 16\n\t"
        "vle64.v        v7, 0(a4)\n\t"
        "vclmul.vv      v4, v3, v5\n\t"
        "vclmulh.vv     v3, v3, v5\n\t"
        "vredxor.vs     v3, v3, v8\n\t"
        "vredxor.vs     v4, v4, v8\n\t"
        "vslideup.vi    v4, v3, 1\n\t"
        "vxor.vv        v3, v4, v7\n\t"

        "addi           a4, a4, 16\n\t"
        "bne            a4, a5, 1b\n\t"

        // Fold 512 bits to 128 bits
        "2:\n\t"
        "addi           t4, t4, 16\n\t"
        "vle64.v        v5, 0(t4)\n\t"
        "vclmul.vv      v6, v0, v5\n\t"
        "vclmulh.vv     v7, v0, v5\n\t"
        "vredxor.vs     v6, v6, v8\n\t"
        "vredxor.vs     v7, v7, v8\n\t"
        "vslideup.vi    v6, v7, 1\n\t"
        "vxor.vv        v0, v6, v1\n\t"

        "vclmul.vv      v6, v0, v5\n\t"
        "vclmulh.vv     v7, v0, v5\n\t"
        "vredxor.vs     v6, v6, v8\n\t"
        "vredxor.vs     v7, v7, v8\n\t"
        "vslideup.vi    v6, v7, 1\n\t"
        "vxor.vv        v0, v6, v2\n\t"

        "vclmul.vv      v6, v0, v5\n\t"
        "vclmulh.vv     v7, v0, v5\n\t"
        "vredxor.vs     v6, v6, v8\n\t"
        "vredxor.vs     v7, v7, v8\n\t"
        "vslideup.vi    v6, v7, 1\n\t"
        "vxor.vv        v0, v6, v3\n\t"

        // Extract 128-bit result from vector register
        "addi           sp, sp, -16\n\t"
        "vse64.v        v0, (sp)\n\t"
        "ld             t0, 0(sp)\n\t"
        "ld             t1, 8(sp)\n\t"
        "addi           sp, sp, 16\n\t"

        // Barrett reduction
        "li             t2, %[const0]\n\t"
        "and            t2, t2, t5\n\t"
        "li             t3, %[const1]\n\t"

        "clmul          t4, t0, t3\n\t"
        "clmulh         t3, t0, t3\n\t"
        "xor            t1, t1, t4\n\t"
        "and            t4, t1, t5\n\t"
        "srli           t1, t1, 32\n\t"
        "clmul          t0, t4, t2\n\t"
        "slli           t3, t3, 32\n\t"
        "xor            t3, t3, t1\n\t"
        "xor            t3, t3, t0\n\t"

        // Final Barrett reduction
        "and            t4, t3, t5\n\t"
        "li             t2, %[quo]\n\t"
        "li             t1, %[poly]\n\t"
        "clmul          t4, t4, t2\n\t"
        "and            t4, t4, t5\n\t"
        "clmul          t4, t4, t1\n\t"
        "xor            t4, t3, t4\n\t"
        "srai           %[result], t4, 32\n\t"
        "and            %[result], %[result], t5\n\t"

        : [result] "=r" (result)
        : [buf] "r" (buf), [len] "r" (len), [crc] "r" (crc), [consts] "r" (fold_consts),
          [const0] "i" (CRC32C_CONST_0), [const1] "i" (CRC32C_CONST_1),
          [quo] "i" (CRC32C_CONST_QUO), [poly] "i" (CRC32C_CONST_POLY)
        : "a3", "a4", "a5", "t0", "t1", "t2", "t3", "t4", "t5", "v0", "v1", "v2", "v3",
          "v4", "v5", "v6", "v7", "v8", "memory"
    );
    size_t tail_len = len % 64;
    if (tail_len > 0){
        result = crc32c_sb8(result, buf + len - tail_len, tail_len);
    }
    return result;
}

/**
 * Hardware-accelerated CRC32 (GZIP/zlib) using RISC-V vector crypto extensions.
 */
static uint32_t crc32_zlib_vclmul(const uint8_t *buf, size_t len, uint32_t crc) {
    if (unlikely(len < 64)) {
        // Fall back to table-based implementation for small buffers
        return crc32_zlib_sb8(crc, buf, len);
    }

    uint32_t result;
    const uint64_t *fold_consts = crc32_fold_const;

    __asm__ __volatile__(
        // Initialize CRC
        "li             t5, 0xffffffff\n\t"
        "and            %[crc], %[crc], t5\n\t"
        "li             a3, 0\n\t"
        "li             t1, 64\n\t"

        // Set vector configuration for 128-bit elements
        "vsetivli       zero, 2, e64, m1, ta, ma\n\t"

        // Load first 64 bytes and initialize
        "mv             a4, %[buf]\n\t"
        "vle64.v        v0, 0(a4)\n\t"
        "addi           a4, a4, 16\n\t"
        "vle64.v        v1, 0(a4)\n\t"
        "addi           a4, a4, 16\n\t"
        "vle64.v        v2, 0(a4)\n\t"
        "addi           a4, a4, 16\n\t"
        "vle64.v        v3, 0(a4)\n\t"
        "addi           a4, a4, 16\n\t"
        "andi           a3, %[len], ~63\n\t"
        "addi           t0, a3, -64\n\t"

        // XOR initial CRC into first vector
        "vmv.s.x        v4, zero\n\t"
        "vmv.s.x        v5, %[crc]\n\t"
        "vslideup.vi    v5, v4, 1\n\t"
        "vxor.vv        v0, v0, v5\n\t"
        "vmv.s.x        v8, zero\n\t"

        // Load folding constant
        "add            a5, a4, t0\n\t"
        "mv             t4, %[consts]\n\t"
        "vle64.v        v5, 0(t4)\n\t"

        // Check if we need main loop
        "addi           t0, %[len], -64\n\t"
        "bltu           t0, t1, 2f\n\t"

        // Main loop - process 64 bytes at a time
        "1:\n\t"
        "vle64.v        v7, 0(a4)\n\t"
        "vclmul.vv      v4, v0, v5\n\t"
        "vclmulh.vv     v0, v0, v5\n\t"
        "vredxor.vs     v0, v0, v8\n\t"
        "vredxor.vs     v4, v4, v8\n\t"
        "vslideup.vi    v4, v0, 1\n\t"
        "vxor.vv        v0, v4, v7\n\t"

        "addi           a4, a4, 16\n\t"
        "vle64.v        v7, 0(a4)\n\t"
        "vclmul.vv      v4, v1, v5\n\t"
        "vclmulh.vv     v1, v1, v5\n\t"
        "vredxor.vs     v1, v1, v8\n\t"
        "vredxor.vs     v4, v4, v8\n\t"
        "vslideup.vi    v4, v1, 1\n\t"
        "vxor.vv        v1, v4, v7\n\t"

        "addi           a4, a4, 16\n\t"
        "vle64.v        v7, 0(a4)\n\t"
        "vclmul.vv      v4, v2, v5\n\t"
        "vclmulh.vv     v2, v2, v5\n\t"
        "vredxor.vs     v2, v2, v8\n\t"
        "vredxor.vs     v4, v4, v8\n\t"
        "vslideup.vi    v4, v2, 1\n\t"
        "vxor.vv        v2, v4, v7\n\t"

        "addi           a4, a4, 16\n\t"
        "vle64.v        v7, 0(a4)\n\t"
        "vclmul.vv      v4, v3, v5\n\t"
        "vclmulh.vv     v3, v3, v5\n\t"
        "vredxor.vs     v3, v3, v8\n\t"
        "vredxor.vs     v4, v4, v8\n\t"
        "vslideup.vi    v4, v3, 1\n\t"
        "vxor.vv        v3, v4, v7\n\t"

        "addi           a4, a4, 16\n\t"
        "bne            a4, a5, 1b\n\t"

        // Fold 512 bits to 128 bits
        "2:\n\t"
        "addi           t4, t4, 16\n\t"
        "vle64.v        v5, 0(t4)\n\t"
        "vclmul.vv      v6, v0, v5\n\t"
        "vclmulh.vv     v7, v0, v5\n\t"
        "vredxor.vs     v6, v6, v8\n\t"
        "vredxor.vs     v7, v7, v8\n\t"
        "vslideup.vi    v6, v7, 1\n\t"
        "vxor.vv        v0, v6, v1\n\t"

        "vclmul.vv      v6, v0, v5\n\t"
        "vclmulh.vv     v7, v0, v5\n\t"
        "vredxor.vs     v6, v6, v8\n\t"
        "vredxor.vs     v7, v7, v8\n\t"
        "vslideup.vi    v6, v7, 1\n\t"
        "vxor.vv        v0, v6, v2\n\t"

        "vclmul.vv      v6, v0, v5\n\t"
        "vclmulh.vv     v7, v0, v5\n\t"
        "vredxor.vs     v6, v6, v8\n\t"
        "vredxor.vs     v7, v7, v8\n\t"
        "vslideup.vi    v6, v7, 1\n\t"
        "vxor.vv        v0, v6, v3\n\t"

        // Extract 128-bit result from vector register
        "addi           sp, sp, -16\n\t"
        "vse64.v        v0, (sp)\n\t"
        "ld             t0, 0(sp)\n\t"
        "ld             t1, 8(sp)\n\t"
        "addi           sp, sp, 16\n\t"

        // Barrett reduction
        "li             t2, %[const0]\n\t"
        "and            t2, t2, t5\n\t"
        "li             t3, %[const1]\n\t"
        "and            t3, t3, t5\n\t"

        "clmul          t4, t0, t3\n\t"
        "clmulh         t3, t0, t3\n\t"
        "xor            t1, t1, t4\n\t"
        "and            t4, t1, t5\n\t"
        "srli           t1, t1, 32\n\t"
        "clmul          t0, t4, t2\n\t"
        "slli           t3, t3, 32\n\t"
        "xor            t3, t3, t1\n\t"
        "xor            t3, t3, t0\n\t"

        // Final Barrett reduction
        "and            t4, t3, t5\n\t"
        "li             t2, %[quo]\n\t"
        "li             t1, %[poly]\n\t"
        "clmul          t4, t4, t2\n\t"
        "and            t4, t4, t5\n\t"
        "clmul          t4, t4, t1\n\t"
        "xor            t4, t3, t4\n\t"
        "srai           %[result], t4, 32\n\t"
        "and            %[result], %[result], t5\n\t"

        : [result] "=r" (result)
        : [buf] "r" (buf), [len] "r" (len), [crc] "r" (crc), [consts] "r" (fold_consts),
          [const0] "i" (CRC32_CONST_0), [const1] "i" (CRC32_CONST_1),
          [quo] "i" (CRC32_CONST_QUO), [poly] "i" (CRC32_CONST_POLY)
        : "a3", "a4", "a5", "t0", "t1", "t2", "t3", "t4", "t5", "v0", "v1", "v2", "v3",
          "v4", "v5", "v6", "v7", "v8", "memory"
    );

    size_t tail_len = len % 64;
    if (tail_len > 0) {
        result = crc32_zlib_sb8(result, buf + len - tail_len, tail_len);
    }
    return result;
}

static void pipelined_crc32c(uint32_t *crc1, uint32_t *crc2, uint32_t *crc3,
                             const uint8_t *p_buf, size_t block_size, int num_blocks) {
    assert(num_blocks >= 1 && num_blocks <= 3 && "invalid num_blocks");

    *crc1 = crc32c_vclmul(p_buf, block_size, *crc1);

    if (num_blocks >= 2) {
        *crc2 = crc32c_vclmul(p_buf + block_size, block_size, *crc2);
    }

    if (num_blocks >= 3) {
        *crc3 = crc32c_vclmul(p_buf + 2 * block_size, block_size, *crc3);
    }
}

static void pipelined_crc32_zlib(uint32_t *crc1, uint32_t *crc2, uint32_t *crc3,
                                 const uint8_t *p_buf, size_t block_size, int num_blocks) {
    assert(num_blocks >= 1 && num_blocks <= 3 && "invalid num_blocks");

    *crc1 = crc32_zlib_vclmul(p_buf, block_size, *crc1);

    if (num_blocks >= 2) {
        *crc2 = crc32_zlib_vclmul(p_buf + block_size, block_size, *crc2);
    }

    if (num_blocks >= 3) {
        *crc3 = crc32_zlib_vclmul(p_buf + 2 * block_size, block_size, *crc3);
    }
}

typedef void (*crc_pipelined_func_t)(uint32_t *, uint32_t *, uint32_t *, const uint8_t *, size_t, int);
extern crc_pipelined_func_t pipelined_crc32c_func;
extern crc_pipelined_func_t pipelined_crc32_zlib_func;

/**
 * Runtime detection of RISC-V vector crypto support.
 */
#include <sys/auxv.h>
#include <asm/hwcap.h>

#include <unistd.h>
#include <sys/syscall.h>
#include <sys/utsname.h>
#include <linux/types.h>
#include <stdio.h>
#include <string.h>

 // riscv_hwprobe definitions for compatibility with older kernels
#ifndef __NR_riscv_hwprobe
#if defined(__riscv) && __riscv_xlen == 64
  #define __NR_riscv_hwprobe 258
#endif
#endif

// RISC-V hardware capability detection
#ifndef COMPAT_HWCAP_ISA_V
#define COMPAT_HWCAP_ISA_V  (1 << ('V' - 'A'))
#endif

// Define riscv_hwprobe structure if not available in headers
#ifndef HAVE_RISCV_HWPROBE
struct riscv_hwprobe {
    __s64 key;
    __u64 value;
};
#endif

// hwprobe key definitions
#ifndef RISCV_HWPROBE_KEY_IMA_EXT_0
#define RISCV_HWPROBE_KEY_IMA_EXT_0      4
#define RISCV_HWPROBE_EXT_ZBC            (1 << 7)
#define RISCV_HWPROBE_EXT_ZVBC           (1 << 18)
#endif

#define REQ_KERNEL_MAJOR 6
#define REQ_KERNEL_MINOR 4

/**
 * Parse kernel version from uname
 */
static int parse_kernel_version(int *major, int *minor, int *patch) {
    struct utsname uts;
    if (uname(&uts) != 0) {
        return -1;
    }

    *patch = 0; // Default value if not parsed
    if (sscanf(uts.release, "%d.%d.%d", major, minor, patch) >= 2) {
        return 0;
    }

    return -1;
}

/**
 * Check if kernel version supports hwprobe
 * hwprobe was introduced in Linux 6.4
 */
static int kernel_supports_hwprobe(void) {
    int major, minor, patch;
    if (parse_kernel_version(&major, &minor, &patch) != 0) {
        return 0; // Unknown, assume not supported
    }

    if (major > REQ_KERNEL_MAJOR) {
        return 1;
    }
    if (major == REQ_KERNEL_MAJOR && minor >= REQ_KERNEL_MINOR) {
        return 1;
    }

    return 0;
}

/**
 * Detect extensions using riscv_hwprobe
 * Returns: -1 on error
 */
static int detect_with_hwprobe(int *has_zbc, int *has_zvbc) {
   #ifdef __NR_riscv_hwprobe
   if (!kernel_supports_hwprobe()) {
       return -1;
   }

   struct riscv_hwprobe probe;
   probe.key = RISCV_HWPROBE_KEY_IMA_EXT_0;
   probe.value = 0;

   long ret = syscall(__NR_riscv_hwprobe, &probe, 1, 0, NULL, 0);
   if (ret != 0) {
       return -1;
   }

   *has_zbc  = (probe.value & RISCV_HWPROBE_EXT_ZBC)  ? 1 : 0;
   *has_zvbc = (probe.value & RISCV_HWPROBE_EXT_ZVBC) ? 1 : 0;

   return 0;
   #else
   return -1;
   #endif
}

/**
 * Check for RISC-V extension support via /proc/cpuinfo
 */
static int check_riscv_extension(const char *extension) {
    FILE *cpuinfo = fopen("/proc/cpuinfo", "r");
    if (!cpuinfo) {
        return 0;
    }

    char line[512];
    int found = 0;

    while (fgets(line, sizeof(line), cpuinfo)) {
       if(strncmp(line, "isa", 3) == 0) {
       if (strstr(line, extension)) {
           found = 1;
       break;
       }
   }
   }

    fclose(cpuinfo);
    return found;
}

/**
 * Detect extensions via /proc/cpuinfo
 */
static int detect_with_cpuinfo(int *has_zbc, int *has_zvbc) {

    *has_zbc = check_riscv_extension("zbc");
    *has_zvbc = check_riscv_extension("zvbc");

    if (*has_zbc == 0 && *has_zvbc == 0) {
        return -1;
    }

    return 0;
}

/**
 * Main detection function with multiple fallback methods
 */
static int detect_crypto_extensions(int *has_zbc, int *has_zvbc) {
    if (has_zbc == NULL || has_zvbc == NULL){
        return -1;
    }

    static int cached_zbc = -1;
    static int cached_zvbc = -1;

    // Return cached results if available
    if (cached_zbc != -1 && cached_zvbc != -1) {
        *has_zbc = cached_zbc;
        *has_zvbc = cached_zvbc;
        return 0;
    }

    // Initialize to not found
    *has_zbc = 0;
    *has_zvbc = 0;

    if (detect_with_hwprobe(has_zbc, has_zvbc) == 0) {
        cached_zbc = *has_zbc;
        cached_zvbc = *has_zvbc;
        return 0;
    }

    if (detect_with_cpuinfo(has_zbc, has_zvbc) < 0) {
        return -1;
    }

    cached_zbc = *has_zbc;
    cached_zvbc = *has_zvbc;
    return 0;
}

/**
 * Constructor function that runs when the library is loaded.
 * Detects hardware support and sets the function pointers accordingly.
 */
void __attribute__((constructor)) init_cpu_support_flag(void) {
    unsigned long auxval = getauxval(AT_HWCAP);
    int has_zbc = 0, has_zvbc = 0;
    if (auxval & COMPAT_HWCAP_ISA_V) {
        if (detect_crypto_extensions(&has_zbc, &has_zvbc) == 0 && has_zbc && has_zvbc) {
            pipelined_crc32c_func = pipelined_crc32c;
            pipelined_crc32_zlib_func = pipelined_crc32_zlib;
        }
    }
}
