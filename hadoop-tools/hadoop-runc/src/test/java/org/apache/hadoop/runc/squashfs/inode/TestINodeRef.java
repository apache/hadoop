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

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestINodeRef {

  private INodeRef ref1;
  private INodeRef ref2;

  @Before
  public void setUp() {
    ref1 = new INodeRef(0x0000_1234_5678_0123L);
    ref2 = new INodeRef(0x1234_5678, (short) 0x123);
  }

  @Test
  public void getLocationShouldReturnSameValueForBothConstructors() {
    assertEquals("wrong location for 1-arg constructor", 0x1234_5678,
        ref1.getLocation());
    assertEquals("wrong location for 2-arg constructor", 0x1234_5678,
        ref2.getLocation());
  }

  @Test
  public void getOffsetShouldReturnSameValueForBothConstructors() {
    assertEquals("wrong offset for 1-arg constructor", (short) 0x123,
        ref1.getOffset());
    assertEquals("wrong offset for 2-arg constructor", (short) 0x123,
        ref2.getOffset());
  }

  @Test
  public void getRawShouldReturnSameValueForBothConstructors() {
    assertEquals("wrong raw value for 1-arg constructor",
        0x0000_1234_5678_0123L, ref1.getRaw());
    assertEquals("wrong raw value for 2-arg constructor",
        0x0000_1234_5678_0123L, ref2.getRaw());
  }

  @Test
  public void toStringShouldNotFail() {
    System.out.println(ref1.toString());
    System.out.println(ref2.toString());
  }

}
