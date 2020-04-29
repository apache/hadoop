<!---
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

Libhdfs++ Coding Standards
==========================

* Libhdfs++ Coding Standards
    * Introduction
    * Automated Formatting
    * Explicit Scoping
    * Comments
    * Portability


Introduction
------------

The foundation of the libhdfs++ project's coding standards
is Google's C++ style guide. It can be found here:

<a href="https://google.github.io/styleguide/cppguide.html">https://google.github.io/styleguide/cppguide.html</a>

There are several small restrictions adopted from Sun's Java
standards and Hadoop convention on top of Google's that must
also be followed as well as portability requirements.

Automated Formatting
--------------------

Prior to submitting a patch for code review use llvm's formatting tool, clang-format, on the .h, .c, and .cc files included in the patch.  Use the -style=google switch when doing so.

Example presubmission usage:

``` shell
cat my_source_file.cc | clang-format -style=goole > temp_file.cc
#optionally diff the source and temp file to get an idea what changed
mv temp_file.cc my_source_file.cc
```

* note: On some linux distributions clang-format already exists in repositories but don't show up without an appended version number.  On Ubuntu you'll find it with:
``` shell
   "apt-get install clang-format-3.6"
```

Explicit Block Scopes
---------------------

Always add brackets conditional and loop bodies, even if the body could fit on a single line.

__BAD__:
``` c
if (foo)
  Bar();

if (foo)
  Bar();
else
  Baz();

for (int i=0; i<10; i++)
  Bar(i);
```
__GOOD__:
``` c
if (foo) {
  Bar();
}

if (foo) {
  Bar();
} else {
  Baz();
}

for (int i=0; i<10; i++) {
  Bar(i);
}
```

Comments
--------

Use the /\* comment \*/ style to maintain consistency with the rest of the Hadoop code base.

__BAD__:
``` c
//this is a bad single line comment
/*
  this is a bad block comment
*/
```
__GOOD__:
``` c
/* this is a single line comment */

/**
 * This is a block comment.  Note that nothing is on the first
 * line of the block.
 **/
```

Portability
-----------

Please make sure you write code that is portable.

* All code most be able to build using GCC and LLVM.
    * In the future we hope to support other compilers as well.
* Don't make assumptions about endianness or architecture.
    * Don't do clever things with pointers or intrinsics.
* Don't write code that could force a non-aligned word access.
    * This causes performance issues on most architectures and isn't supported at all on some.
    * Generally the compiler will prevent this unless you are doing clever things with pointers e.g. abusing placement new or reinterpreting a pointer into a pointer to a wider type.
* If a type needs to be a a specific width make sure to specify it.
    * `int32_t my_32_bit_wide_int`
* Avoid using compiler dependent pragmas or attributes.
    * If there is a justified and unavoidable reason for using these you must document why. See examples below.

__BAD__:
``` c
struct Foo {
  int32_t x_;
  char y_;
  int32_t z_;
  char z_;
} __attribute__((packed));
/**
 * "I didn't profile and identify that this is causing
 * significant memory overhead but I want to pack it to
 * save 6 bytes"
 **/
```
__NECESSARY__: Still not good but required for short-circuit reads.
``` c
struct FileDescriptorMessage {
  struct cmsghdr msg_;
  int file_descriptors_[2];
} __attribute__((packed));
/**
 * This is actually needed for short circuit reads.
 * "struct cmsghdr" is well defined on UNIX systems.
 * This mechanism relies on the fact that any passed
 * ancillary data is _directly_ following the cmghdr.
 * The kernel interprets any padding as real data.
 **/
```
