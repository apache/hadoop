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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

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

  public void testUMaskParser() throws IOException {
    Configuration conf = new Configuration();
    
    // Ensure that we get the right octal values back for all legal values
    for(FsAction u : FsAction.values()) {
      for(FsAction g : FsAction.values()) {
        for(FsAction o : FsAction.values()) {
          FsPermission f = new FsPermission(u, g, o);
          String asOctal = String.format("%1$03o", f.toShort());
          conf.set(FsPermission.UMASK_LABEL, asOctal);
          FsPermission fromConf = FsPermission.getUMask(conf);
          assertEquals(f, fromConf);
        }
      }
    }
  }

  public void TestSymbolicUmasks() {
    Configuration conf = new Configuration();
    
    // Test some symbolic settings       Setting       Octal result
    String [] symbolic = new String [] { "a+rw",        "666",
                                         "u=x,g=r,o=w", "142",
                                         "u=x",         "100" };
    
    for(int i = 0; i < symbolic.length; i += 2) {
      conf.set(FsPermission.UMASK_LABEL, symbolic[i]);
      short val = Short.valueOf(symbolic[i + 1], 8);
      assertEquals(val, FsPermission.getUMask(conf).toShort());
    }
  }

  public void testBadUmasks() {
    Configuration conf = new Configuration();
    
    for(String b : new String [] {"1777", "22", "99", "foo", ""}) {
      conf.set(FsPermission.UMASK_LABEL, b); 
      try {
        FsPermission.getUMask(conf);
        fail("Shouldn't have been able to parse bad umask");
      } catch(IllegalArgumentException iae) {
        assertEquals(iae.getMessage(), b);
      }
    }
  }
  
  // Ensure that when the deprecated decimal umask key is used, it is correctly
  // parsed as such and converted correctly to an FsPermission value
  public void testDeprecatedUmask() {
    Configuration conf = new Configuration();
    conf.set(FsPermission.DEPRECATED_UMASK_LABEL, "302"); // 302 = 0456
    FsPermission umask = FsPermission.getUMask(conf);

    assertEquals(0456, umask.toShort());
  }
}
