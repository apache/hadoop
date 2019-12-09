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

import org.junit.Test;
import static org.junit.Assert.*;

import com.google.re2j.PatternSyntaxException;
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
    assertMatch(true, "*", "^$", "foo", "bar", "\n");
    assertMatch(true, "?", "?", "^", "[", "]", "$");
    assertMatch(true, "foo*", "foo", "food", "fool", "foo\n", "foo\nbar");
    assertMatch(true, "f*d", "fud", "food", "foo\nd");
    assertMatch(true, "*d", "good", "bad", "\nd");
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
    shouldThrow("[", "[[]]", "{", "\\");
  }

  @Test(timeout=10000) public void testPathologicalPatterns() {
    String badFilename = "job_1429571161900_4222-1430338332599-tda%2D%2D+******************************+++...%270%27%28Stage-1430338580443-39-2000-SUCCEEDED-production%2Dhigh-1430338340360.jhist";
    assertMatch(true, badFilename, badFilename);
  }
}
