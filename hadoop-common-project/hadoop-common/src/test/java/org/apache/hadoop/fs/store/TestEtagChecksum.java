/*
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

package org.apache.hadoop.fs.store;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

/**
 * Unit test of etag operations.
 */
public class TestEtagChecksum extends Assert {

  private final EtagChecksum empty1 = tag("");
  private final EtagChecksum empty2 = tag("");
  private final EtagChecksum valid1 = tag("valid");
  private final EtagChecksum valid2 = tag("valid");

  @Test
  public void testEmptyTagsEqual() {
    assertEquals(empty1, empty2);
  }

  @Test
  public void testEmptyTagRoundTrip() throws Throwable {
    assertEquals(empty1, roundTrip(empty1));
  }

  @Test
  public void testValidTagsEqual() {
    assertEquals(valid1, valid2);
  }

  @Test
  public void testValidTagRoundTrip() throws Throwable {
    assertEquals(valid1, roundTrip(valid1));
  }

  @Test
  public void testValidAndEmptyTagsDontMatch() {
    assertNotEquals(valid1, empty1);
    assertNotEquals(valid1, tag("other valid one"));
  }

  @Test
  public void testDifferentTagsDontMatch() {
    assertNotEquals(valid1, tag("other valid one"));
  }

  private EtagChecksum tag(String t) {
    return new EtagChecksum(t);
  }

  private EtagChecksum roundTrip(EtagChecksum tag) throws IOException {
    try (DataOutputBuffer dob = new DataOutputBuffer();
         DataInputBuffer dib = new DataInputBuffer()) {
      tag.write(dob);
      dib.reset(dob.getData(), dob.getLength());
      EtagChecksum t2 = new EtagChecksum();
      t2.readFields(dib);
      return t2;
    }
  }

}
