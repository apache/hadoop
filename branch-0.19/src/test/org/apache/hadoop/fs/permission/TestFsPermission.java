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
package org.apache.hadoop.fs.permission;

import junit.framework.TestCase;

import static org.apache.hadoop.fs.permission.FsAction.*;

public class TestFsPermission extends TestCase {
  public void testFsAction() {
    //implies
    for(FsAction a : FsAction.values()) {
      assertTrue(ALL.implies(a));
    }
    for(FsAction a : FsAction.values()) {
      assertTrue(a == NONE? NONE.implies(a): !NONE.implies(a));
    }
    for(FsAction a : FsAction.values()) {
      assertTrue(a == READ_EXECUTE || a == READ || a == EXECUTE || a == NONE?
          READ_EXECUTE.implies(a): !READ_EXECUTE.implies(a));
    }

    //masks
    assertEquals(EXECUTE, EXECUTE.and(READ_EXECUTE));
    assertEquals(READ, READ.and(READ_EXECUTE));
    assertEquals(NONE, WRITE.and(READ_EXECUTE));

    assertEquals(READ, READ_EXECUTE.and(READ_WRITE));
    assertEquals(NONE, READ_EXECUTE.and(WRITE));
    assertEquals(WRITE_EXECUTE, ALL.and(WRITE_EXECUTE));
  }

  public void testFsPermission() {
    for(short s = 0; s < (1<<9); s++) {
      assertEquals(s, new FsPermission(s).toShort());
    }

    String symbolic = "-rwxrwxrwx";
    StringBuilder b = new StringBuilder("-123456789");
    for(int i = 0; i < (1<<9); i++) {
      for(int j = 1; j < 10; j++) {
        b.setCharAt(j, '-');
      }
      String binary = Integer.toBinaryString(i);
      int len = binary.length();
      for(int j = 0; j < len; j++) {
        if (binary.charAt(j) == '1') {
          int k = 9 - (len - 1 - j);
          b.setCharAt(k, symbolic.charAt(k));
        }
      }
      assertEquals(i, FsPermission.valueOf(b.toString()).toShort());
    }
  }
}
