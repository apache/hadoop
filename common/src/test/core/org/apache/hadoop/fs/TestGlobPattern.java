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

package org.apache.hadoop.fs;

import java.util.regex.PatternSyntaxException;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Tests for glob patterns
 */
public class TestGlobPattern {
  private void assertMatch(boolean yes, String glob, String...input) {
    GlobPattern pattern = new GlobPattern(glob);

    for (String s : input) {
      boolean result = pattern.matches(s);
      assertTrue(glob +" should"+ (yes ? "" : " not") +" match "+ s,
                 yes ? result : !result);
    }
  }

  private void shouldThrow(String... globs) {
    for (String glob : globs) {
      try {
        GlobPattern.compile(glob);
      }
      catch (PatternSyntaxException e) {
        e.printStackTrace();
        continue;
      }
      assertTrue("glob "+ glob +" should throw", false);
    }
  }

  @Test public void testValidPatterns() {
    assertMatch(true, "*", "^$", "foo", "bar");
    assertMatch(true, "?", "?", "^", "[", "]", "$");
    assertMatch(true, "foo*", "foo", "food", "fool");
    assertMatch(true, "f*d", "fud", "food");
    assertMatch(true, "*d", "good", "bad");
    assertMatch(true, "\\*\\?\\[\\{\\\\", "*?[{\\");
    assertMatch(true, "[]^-]", "]", "-", "^");
    assertMatch(true, "]", "]");
    assertMatch(true, "^.$()|+", "^.$()|+");
    assertMatch(true, "[^^]", ".", "$", "[", "]");
    assertMatch(false, "[^^]", "^");
    assertMatch(true, "[!!-]", "^", "?");
    assertMatch(false, "[!!-]", "!", "-");
    assertMatch(true, "{[12]*,[45]*,[78]*}", "1", "2!", "4", "42", "7", "7$");
    assertMatch(false, "{[12]*,[45]*,[78]*}", "3", "6", "9ß");
    assertMatch(true, "}", "}");
  }

  @Test public void testInvalidPatterns() {
    shouldThrow("[", "[[]]", "[][]", "{", "\\");
  }
}
