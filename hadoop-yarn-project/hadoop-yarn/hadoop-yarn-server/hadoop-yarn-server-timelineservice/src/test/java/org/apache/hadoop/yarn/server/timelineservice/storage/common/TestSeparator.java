/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Iterables;

public class TestSeparator {

  private static String villain = "Dr. Heinz Doofenshmirtz";
  private static String special =
      ".   *   |   ?   +   (   )   [   ]   {   }   ^   $  \\ \"";

  /**
   *
   */
  @Test
  public void testEncodeDecodeString() {

    for (Separator separator : Separator.values()) {
      testEncodeDecode(separator, "");
      testEncodeDecode(separator, " ");
      testEncodeDecode(separator, "!");
      testEncodeDecode(separator, "?");
      testEncodeDecode(separator, "&");
      testEncodeDecode(separator, "+");
      testEncodeDecode(separator, "Dr.");
      testEncodeDecode(separator, "Heinz");
      testEncodeDecode(separator, "Doofenshmirtz");
      testEncodeDecode(separator, villain);
      testEncodeDecode(separator, special);

      assertNull(separator.encode(null));

    }
  }

  private void testEncodeDecode(Separator separator, String token) {
    String encoded = separator.encode(token);
    String decoded = separator.decode(encoded);
    String msg = "token:" + token + " separator:" + separator + ".";
    assertEquals(msg, token, decoded);
  }

  @Test
  public void testEncodeDecode() {
    testEncodeDecode("Dr.", Separator.QUALIFIERS);
    testEncodeDecode("Heinz", Separator.QUALIFIERS, Separator.QUALIFIERS);
    testEncodeDecode("Doofenshmirtz", Separator.QUALIFIERS, null,
        Separator.QUALIFIERS);
    testEncodeDecode("&Perry", Separator.QUALIFIERS, Separator.VALUES, null);
    testEncodeDecode("the ", Separator.QUALIFIERS, Separator.SPACE);
    testEncodeDecode("Platypus...", (Separator) null);
    testEncodeDecode("The what now ?!?", Separator.QUALIFIERS,
        Separator.VALUES, Separator.SPACE);

  }

  /**
   * Simple test to encode and decode using the same separators and confirm that
   * we end up with the same as what we started with.
   *
   * @param token
   * @param separators
   */
  private static void testEncodeDecode(String token, Separator... separators) {
    byte[] encoded = Separator.encode(token, separators);
    String decoded = Separator.decode(encoded, separators);
    assertEquals(token, decoded);
  }

  @Test
  public void testJoinStripped() {
    List<String> stringList = new ArrayList<String>(0);
    stringList.add("nothing");

    String joined = Separator.VALUES.joinEncoded(stringList);
    Iterable<String> split = Separator.VALUES.splitEncoded(joined);
    assertTrue(Iterables.elementsEqual(stringList, split));

    stringList = new ArrayList<String>(3);
    stringList.add("a");
    stringList.add("b?");
    stringList.add("c");

    joined = Separator.VALUES.joinEncoded(stringList);
    split = Separator.VALUES.splitEncoded(joined);
    assertTrue(Iterables.elementsEqual(stringList, split));

    String[] stringArray1 = { "else" };
    joined = Separator.VALUES.joinEncoded(stringArray1);
    split = Separator.VALUES.splitEncoded(joined);
    assertTrue(Iterables.elementsEqual(Arrays.asList(stringArray1), split));

    String[] stringArray2 = { "d", "e?", "f" };
    joined = Separator.VALUES.joinEncoded(stringArray2);
    split = Separator.VALUES.splitEncoded(joined);
    assertTrue(Iterables.elementsEqual(Arrays.asList(stringArray2), split));

    List<String> empty = new ArrayList<String>(0);
    split = Separator.VALUES.splitEncoded(null);
    assertTrue(Iterables.elementsEqual(empty, split));

  }

}
