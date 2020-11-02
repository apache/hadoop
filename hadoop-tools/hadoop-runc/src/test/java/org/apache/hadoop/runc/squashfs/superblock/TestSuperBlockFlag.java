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

package org.apache.hadoop.runc.squashfs.superblock;

import org.junit.Test;

import java.util.EnumSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestSuperBlockFlag {

  @Test
  public void maskShouldReturnSuccessivePowersOfTwo() {
    SuperBlockFlag[] values = SuperBlockFlag.values();

    int mask = 1;
    for (int i = 0; i < values.length; i++) {
      assertEquals(String.format("Wrong mask for %s", values[i]), mask,
          values[i].mask());
      mask = mask << 1;
    }
  }

  @Test
  public void isSetShouldReturnTrueIfMaskIsPresent() {
    SuperBlockFlag[] values = SuperBlockFlag.values();

    short mask = 1;
    for (int i = 0; i < values.length; i++) {
      assertTrue(String.format("Wrong isSet() for %s", values[i]),
          values[i].isSet(mask));
      mask = (short) (mask << 1);
    }
  }

  @Test
  public void isSetShouldReturnFalseIfMaskIsNotPresent() {
    SuperBlockFlag[] values = SuperBlockFlag.values();

    short mask = 1;
    for (int i = 0; i < values.length; i++) {
      assertFalse(String.format("Wrong isSet() for %s", values[i]),
          values[i].isSet((short) (~mask & 0xffff)));
      mask = (short) (mask << 1);
    }
  }

  @Test
  public void flagsPresentShouldIncludeValueContainingMask() {
    SuperBlockFlag[] values = SuperBlockFlag.values();

    short mask = 1;
    for (int i = 0; i < values.length; i++) {
      EnumSet<SuperBlockFlag> flags = SuperBlockFlag.flagsPresent(mask);
      assertTrue(String.format("Flag not present for %s", values[i]),
          flags.contains(values[i]));
      mask = (short) (mask << 1);
    }
  }

  @Test
  public void flagsPresentShouldNotIncludeValueNotContainingMask() {
    SuperBlockFlag[] values = SuperBlockFlag.values();

    short mask = 1;
    for (int i = 0; i < values.length; i++) {
      EnumSet<SuperBlockFlag> flags =
          SuperBlockFlag.flagsPresent((short) (~mask & 0xffff));
      assertFalse(String.format("Flag present for %s", values[i]),
          flags.contains(values[i]));
      mask = (short) (mask << 1);
    }
  }

  @Test
  public void flagsForCollectionShouldIncludePassedInValue() {
    SuperBlockFlag[] values = SuperBlockFlag.values();

    short mask = 1;
    for (int i = 0; i < values.length; i++) {
      short value = SuperBlockFlag.flagsFor(EnumSet.of(values[i]));
      assertEquals(String.format("Wrong result for %s", values[i]), mask,
          value);
      mask = (short) (mask << 1);
    }
  }

  @Test
  public void flagsForVarArgsShouldIncludePassedInValue() {
    SuperBlockFlag[] values = SuperBlockFlag.values();

    short mask = 1;
    for (int i = 0; i < values.length; i++) {
      short value = SuperBlockFlag.flagsFor(values[i]);
      assertEquals(String.format("Wrong result for %s", values[i]), mask,
          value);
      mask = (short) (mask << 1);
    }
  }

  @Test
  public void flagsForCollectionShouldIncludeMultipleFlags() {
    short mask = SuperBlockFlag.flagsFor(
        EnumSet.of(SuperBlockFlag.EXPORTABLE, SuperBlockFlag.DUPLICATES));
    assertEquals("Wrong mask",
        SuperBlockFlag.EXPORTABLE.mask() | SuperBlockFlag.DUPLICATES.mask(),
        mask);
  }

  @Test
  public void flagsForVarargsShouldIncludeMultipleFlags() {
    short mask = SuperBlockFlag
        .flagsFor(SuperBlockFlag.EXPORTABLE, SuperBlockFlag.DUPLICATES);
    assertEquals("Wrong mask",
        SuperBlockFlag.EXPORTABLE.mask() | SuperBlockFlag.DUPLICATES.mask(),
        mask);
  }

}
