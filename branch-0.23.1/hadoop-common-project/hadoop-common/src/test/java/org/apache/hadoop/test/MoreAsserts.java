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

package org.apache.hadoop.test;

import java.util.Iterator;
import org.junit.Assert;

/**
 * A few more asserts
 */
public class MoreAsserts {

  /**
   * Assert equivalence for array and iterable
   * @param <T> the type of the elements
   * @param s the name/message for the collection
   * @param expected  the expected array of elements
   * @param actual    the actual iterable of elements
   */
  public static <T> void assertEquals(String s, T[] expected,
                                      Iterable<T> actual) {
    Iterator<T> it = actual.iterator();
    int i = 0;
    for (; i < expected.length && it.hasNext(); ++i) {
      Assert.assertEquals("Element "+ i +" for "+ s, expected[i], it.next());
    }
    Assert.assertTrue("Expected more elements", i == expected.length);
    Assert.assertTrue("Expected less elements", !it.hasNext());
  }

  /**
   * Assert equality for two iterables
   * @param <T> the type of the elements
   * @param s
   * @param expected
   * @param actual
   */
  public static <T> void assertEquals(String s, Iterable<T> expected,
                                      Iterable<T> actual) {
    Iterator<T> ite = expected.iterator();
    Iterator<T> ita = actual.iterator();
    int i = 0;
    while (ite.hasNext() && ita.hasNext()) {
      Assert.assertEquals("Element "+ i +" for "+s, ite.next(), ita.next());
    }
    Assert.assertTrue("Expected more elements", !ite.hasNext());
    Assert.assertTrue("Expected less elements", !ita.hasNext());
  }

}
