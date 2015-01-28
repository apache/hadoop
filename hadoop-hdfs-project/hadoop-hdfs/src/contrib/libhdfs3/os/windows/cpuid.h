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

/*
 * Implement get_cpuid() in Windows.
 */

#ifndef _LIBHDFS3_CPUID_HEADER_
#define _LIBHDFS3_CPUID_HEADER_

#include <intrin.h>

#if ((defined(__X86__) || defined(__i386__) || defined(i386) || defined(_M_IX86) || defined(__386__) || defined(__x86_64__) || defined(_M_X64)))
int __get_cpuid(unsigned int __level, unsigned int *__eax,
    unsigned int *__ebx, unsigned int *__ecx, unsigned int *__edx) {
    int CPUInfo[4] = { *__eax, *__ebx, *__ecx, *__edx };
    __cpuid(CPUInfo, 0);
    // check if the CPU supports the cpuid instruction.
    if (CPUInfo[0] != 0) {
        __cpuid(CPUInfo, __level);
        *__eax = CPUInfo[0];
        *__ebx = CPUInfo[1];
        *__ecx = CPUInfo[2];
        *__edx = CPUInfo[3];
        return 1;
	}
    return 0;
}
#else
#error Unimplemented __get_cpuid() for non-x86 processor!
#endif

#endif
