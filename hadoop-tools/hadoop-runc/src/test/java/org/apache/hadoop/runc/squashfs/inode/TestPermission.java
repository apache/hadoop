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

package org.apache.hadoop.runc.squashfs.inode;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestPermission {

  @Test
  public void descriptionShouldReturnSomethingForAllValues() {
    for (Permission p : Permission.values()) {
      assertNotNull(String.format("Null description found for %s", p),
          p.description());
    }
  }

  @Test
  public void bitShouldBeInAscendingOrderForAllValues() {
    int bit = 0;
    for (Permission p : Permission.values()) {
      assertEquals(String.format("Wrong bit value found for %s", p), bit,
          p.bit());
      bit++;
    }
  }

  @Test
  public void maskShouldIncreaseByPowersOfTwoForAllValues() {
    int mask = 1;
    for (Permission p : Permission.values()) {
      assertEquals(String.format("Wrong mask value found for %s", p), mask,
          p.mask());
      mask = (mask << 1);
    }
  }

  @Test
  public void toDisplayShouldSucceedForCommonValues() {
    assertEquals("rwxr-xr-x", Permission.toDisplay(Permission.from(0755)));
    assertEquals("rw-r--r--", Permission.toDisplay(Permission.from(0644)));
    assertEquals("---------", Permission.toDisplay(Permission.from(0000)));
    assertEquals("rwxrwxrwx", Permission.toDisplay(Permission.from(0777)));
    assertEquals("rwxrwxrwt", Permission.toDisplay(Permission.from(01777)));
    assertEquals("rwxrwsrwx", Permission.toDisplay(Permission.from(02777)));
    assertEquals("rwsrwxrwx", Permission.toDisplay(Permission.from(04777)));
    assertEquals("rw-r--r-T", Permission.toDisplay(Permission.from(01644)));
    assertEquals("rw-r-Sr--", Permission.toDisplay(Permission.from(02644)));
    assertEquals("rwSr--r--", Permission.toDisplay(Permission.from(04644)));
  }

  @Test
  public void toValueShouldSucceedForCommonValues() {
    assertEquals(0755, Permission.toValue(Permission.from("rwxr-xr-x")));
    assertEquals(0644, Permission.toValue(Permission.from("rw-r--r--")));
    assertEquals(0000, Permission.toValue(Permission.from("---------")));
    assertEquals(0777, Permission.toValue(Permission.from("rwxrwxrwx")));
    assertEquals(01777, Permission.toValue(Permission.from("rwxrwxrwt")));
    assertEquals(02777, Permission.toValue(Permission.from("rwxrwsrwx")));
    assertEquals(04777, Permission.toValue(Permission.from("rwsrwxrwx")));
    assertEquals(01644, Permission.toValue(Permission.from("rw-r--r-T")));
    assertEquals(02644, Permission.toValue(Permission.from("rw-r-Sr--")));
    assertEquals(04644, Permission.toValue(Permission.from("rwSr--r--")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void toValueShouldFailForStringsLessThan9Chars() {
    Permission.toValue("rwxrwxr-");
  }

  @Test
  public void toDisplayAndToValueShouldBeReflexive() {
    for (int i = 0; i < 07777; i++) {
      String display = Permission.toDisplay(i);
      int value = Permission.toValue(display);
      assertEquals(String
          .format("Mismatch between toDisplay('%s') and toValue(%o)", display,
              i), i, value);
    }
  }

}
