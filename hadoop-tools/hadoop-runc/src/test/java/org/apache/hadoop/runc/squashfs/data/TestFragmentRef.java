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

package org.apache.hadoop.runc.squashfs.data;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestFragmentRef {

  private FragmentRef ref;

  @Before
  public void setUp() {
    ref = new FragmentRef(1);
  }

  @Test
  public void getOffsetShouldReturnConstructedValue() {
    assertEquals(1, ref.getOffset());
  }

  @Test
  public void getFragmentIndexShouldInitiallyReturnInvalidValue() {
    assertEquals(-1, ref.getFragmentIndex());
  }

  @Test
  public void isValidShouldReturnFalseUntilCommitIsCalled() {
    assertFalse(ref.isValid());
    ref.commit(2);
    assertTrue(ref.isValid());
  }

  @Test
  public void commitShouldUpdateFragmentIndex() {
    ref.commit(2);
    assertEquals(2, ref.getFragmentIndex());
  }

  @Test
  public void toStringShouldNotFail() {
    System.out.println(ref.toString());
  }

  @Test
  public void toStringShouldNotFailAfterCommit() {
    ref.commit(2);
    System.out.println(ref.toString());
  }

}
