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

#ifndef LIBHDFSPP_CROSS_PLATFORM_TYPES_TEST
#define LIBHDFSPP_CROSS_PLATFORM_TYPES_TEST

#include <limits>

#include <gtest/gtest.h>

/**
 * {@class XPlatformTypesTest} tests the types defined in the XPlatform library.
 */
template <class T> class XPlatformTypesTest : public testing::Test {
public:
  XPlatformTypesTest() = default;
  XPlatformTypesTest(const XPlatformTypesTest &) = delete;
  XPlatformTypesTest(XPlatformTypesTest &&) = delete;
  XPlatformTypesTest &operator=(const XPlatformTypesTest &) = delete;
  XPlatformTypesTest &operator=(XPlatformTypesTest &&) = delete;
  ~XPlatformTypesTest() override;
};

template <class T> XPlatformTypesTest<T>::~XPlatformTypesTest() = default;

TYPED_TEST_SUITE_P(XPlatformTypesTest);

/**
 * Tests whether ssize_t can hold -1.
 */
TYPED_TEST_P(XPlatformTypesTest, SSizeTMinusOne) {
  constexpr TypeParam value = -1;
  ASSERT_EQ(value, -1);
}

/**
 * Tests whether ssize_t can hold at least an int.
 */
TYPED_TEST_P(XPlatformTypesTest, SSizeTCanHoldInts) {
  constexpr auto actual = std::numeric_limits<TypeParam>::max();
  constexpr auto expected = std::numeric_limits<int>::max();
  ASSERT_GE(actual, expected);
}

// For 64-bit systems.
#if _WIN64 || __x86_64__ || __ppc64__
/**
 * Tests whether ssize_t can hold at least a long int.
 */
TYPED_TEST_P(XPlatformTypesTest, SSizeTCanHoldLongInts) {
  constexpr auto actual = std::numeric_limits<TypeParam>::max();
  constexpr auto expected = std::numeric_limits<long int>::max();
  ASSERT_GE(actual, expected);
}

REGISTER_TYPED_TEST_SUITE_P(XPlatformTypesTest, SSizeTMinusOne,
                            SSizeTCanHoldInts, SSizeTCanHoldLongInts);
#else
REGISTER_TYPED_TEST_SUITE_P(XPlatformTypesTest, SSizeTMinusOne,
                            SSizeTCanHoldInts);
#endif

#endif