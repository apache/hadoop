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

package org.apache.hadoop.runc.squashfs.metadata;

import org.apache.hadoop.runc.squashfs.inode.INodeRef;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestMetadataBlockRef {

  private MetadataBlockRef ref;

  @Before
  public void setUp() {
    ref = new MetadataBlockRef(1, (short) 2);
  }

  @Test
  public void getLocationShouldReturnConstructedValue() {
    assertEquals("wrong value", 1, ref.getLocation());
  }

  @Test
  public void getOffsetShouldReturnConstructedValue() {
    assertEquals("wrong value", (short) 2, ref.getOffset());
  }

  @Test
  public void toStringShouldNotFail() {
    System.out.println(ref.toString());
  }

  @Test
  public void testToINodeRef() {
    INodeRef inodeRef = ref.toINodeRef();
    assertEquals(1, inodeRef.getLocation());
    assertEquals((short) 2, inodeRef.getOffset());
  }

  @Test
  public void testToINodeRefRaw() {
    long raw = ref.toINodeRefRaw();

    INodeRef inodeRef = new INodeRef(raw);
    assertEquals(1, inodeRef.getLocation());
    assertEquals((short) 2, inodeRef.getOffset());
  }

}
