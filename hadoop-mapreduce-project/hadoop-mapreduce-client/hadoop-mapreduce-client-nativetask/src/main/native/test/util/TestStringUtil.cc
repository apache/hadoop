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

#include "util/StringUtil.h"
#include "test_commons.h"

TEST(StringUtil, Convertion) {
  ASSERT_FLOAT_EQ(StringUtil::toFloat("1.333"), 1.333);
  ASSERT_FLOAT_EQ(StringUtil::toFloat(StringUtil::ToString(1.333f)), 1.333);
  ASSERT_EQ(StringUtil::ToString(76957382U), "76957382");
  ASSERT_EQ(StringUtil::ToString((uint64_t )76957382234233432ULL), "76957382234233432");
  ASSERT_EQ(StringUtil::ToString(111, ' ', 40), "                                     111");
}

TEST(StringUtil, ToHexString) {
  uint8_t buff[4] = {'i', 'j', 'k', 'l'};
  ASSERT_EQ(StringUtil::ToHexString(buff, 4), string("696a6b6c"));
}

TEST(StringUtil, Format) {
  string t = StringUtil::Format("%d %d %d %.3lf %s", 1, 2, 3, 1.333, "aaaaaaaaaaa");
  ASSERT_EQ(t, "1 2 3 1.333 aaaaaaaaaaa");
  string longstring(999, 'a');
  string d = StringUtil::Format("%s", longstring.c_str());
  ASSERT_EQ(longstring, d);
}

TEST(StringUtil, Trim) {
  ASSERT_EQ(StringUtil::Trim("  \taaaa  \t  "), "aaaa");
  ASSERT_EQ(StringUtil::Trim("  \t  \t  "), "");
  ASSERT_EQ(StringUtil::Trim(""), "");
}

TEST(StringUtil, ToLower) {
  ASSERT_EQ(StringUtil::ToLower("111ABabABabAbaB222"), "111abababababab222");
  ASSERT_EQ(StringUtil::ToLower(""), "");
}

TEST(StringUtil, JoinSplit) {
  vector<string> temp1, temp2, temp3, temp4;
  StringUtil::Split("1aaa bbb ccc", " ", temp1, false);
  StringUtil::Split("  1aaa  bbb  ccc ", " ", temp2, true);
  ASSERT_EQ(temp1, temp2);
  string j = StringUtil::Join(temp1, ",");
  ASSERT_EQ(j, "1aaa,bbb,ccc");
  StringUtil::Split("  a b ", " ", temp3, false);
  ASSERT_EQ(temp3, MakeStringArray(temp4, "", "", "a", "b", "", NULL));
}

