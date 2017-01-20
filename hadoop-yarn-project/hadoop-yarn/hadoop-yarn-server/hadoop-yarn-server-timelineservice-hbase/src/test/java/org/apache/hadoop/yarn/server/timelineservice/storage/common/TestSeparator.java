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
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.google.common.collect.Iterables;

public class TestSeparator {

  private static String villain = "Dr. Heinz Doofenshmirtz";
  private static String special =
      ".   *   |   ?   +   \t   (   )   [   ]   {   }   ^   $  \\ \"  %";

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
      testEncodeDecode(separator, "\t");
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
  @Test
  public void testEncodedValues() {
    testEncodeDecode("Double-escape %2$ and %9$ or %%2$ or %%3$, nor  %%%2$" +
        "= no problem!",
        Separator.QUALIFIERS, Separator.VALUES, Separator.SPACE, Separator.TAB);
  }

  @Test
  public void testSplits() {
    byte[] maxLongBytes = Bytes.toBytes(Long.MAX_VALUE);
    byte[] maxIntBytes = Bytes.toBytes(Integer.MAX_VALUE);
    for (Separator separator : Separator.values()) {
      String str1 = "cl" + separator.getValue() + "us";
      String str2 = separator.getValue() + "rst";
      byte[] sepByteArr = Bytes.toBytes(separator.getValue());
      byte[] longVal1Arr = Bytes.add(sepByteArr, Bytes.copy(maxLongBytes,
          sepByteArr.length, Bytes.SIZEOF_LONG - sepByteArr.length));
      byte[] intVal1Arr = Bytes.add(sepByteArr, Bytes.copy(maxIntBytes,
          sepByteArr.length, Bytes.SIZEOF_INT - sepByteArr.length));
      byte[] arr = separator.join(
          Bytes.toBytes(separator.encode(str1)), longVal1Arr,
          Bytes.toBytes(separator.encode(str2)), intVal1Arr);
      int[] sizes = {Separator.VARIABLE_SIZE, Bytes.SIZEOF_LONG,
          Separator.VARIABLE_SIZE, Bytes.SIZEOF_INT};
      byte[][] splits = separator.split(arr, sizes);
      assertEquals(4, splits.length);
      assertEquals(str1, separator.decode(Bytes.toString(splits[0])));
      assertEquals(Bytes.toLong(longVal1Arr), Bytes.toLong(splits[1]));
      assertEquals(str2, separator.decode(Bytes.toString(splits[2])));
      assertEquals(Bytes.toInt(intVal1Arr), Bytes.toInt(splits[3]));

      longVal1Arr = Bytes.add(Bytes.copy(maxLongBytes, 0, Bytes.SIZEOF_LONG -
          sepByteArr.length), sepByteArr);
      intVal1Arr = Bytes.add(Bytes.copy(maxIntBytes, 0, Bytes.SIZEOF_INT -
          sepByteArr.length), sepByteArr);
      arr = separator.join(Bytes.toBytes(separator.encode(str1)), longVal1Arr,
          Bytes.toBytes(separator.encode(str2)), intVal1Arr);
      splits = separator.split(arr, sizes);
      assertEquals(4, splits.length);
      assertEquals(str1, separator.decode(Bytes.toString(splits[0])));
      assertEquals(Bytes.toLong(longVal1Arr), Bytes.toLong(splits[1]));
      assertEquals(str2, separator.decode(Bytes.toString(splits[2])));
      assertEquals(Bytes.toInt(intVal1Arr), Bytes.toInt(splits[3]));

      longVal1Arr = Bytes.add(sepByteArr, Bytes.copy(maxLongBytes,
          sepByteArr.length, 4 - sepByteArr.length), sepByteArr);
      longVal1Arr = Bytes.add(longVal1Arr, Bytes.copy(maxLongBytes, 4, 3 -
              sepByteArr.length), sepByteArr);
      arr = separator.join(Bytes.toBytes(separator.encode(str1)), longVal1Arr,
          Bytes.toBytes(separator.encode(str2)), intVal1Arr);
      splits = separator.split(arr, sizes);
      assertEquals(4, splits.length);
      assertEquals(str1, separator.decode(Bytes.toString(splits[0])));
      assertEquals(Bytes.toLong(longVal1Arr), Bytes.toLong(splits[1]));
      assertEquals(str2, separator.decode(Bytes.toString(splits[2])));
      assertEquals(Bytes.toInt(intVal1Arr), Bytes.toInt(splits[3]));

      arr = separator.join(Bytes.toBytes(separator.encode(str1)),
          Bytes.toBytes(separator.encode(str2)), intVal1Arr, longVal1Arr);
      int[] sizes1 = {Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE,
          Bytes.SIZEOF_INT, Bytes.SIZEOF_LONG};
      splits = separator.split(arr, sizes1);
      assertEquals(4, splits.length);
      assertEquals(str1, separator.decode(Bytes.toString(splits[0])));
      assertEquals(str2, separator.decode(Bytes.toString(splits[1])));
      assertEquals(Bytes.toInt(intVal1Arr), Bytes.toInt(splits[2]));
      assertEquals(Bytes.toLong(longVal1Arr), Bytes.toLong(splits[3]));

      try {
        int[] sizes2 = {Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE,
            Bytes.SIZEOF_INT, 7};
        splits = separator.split(arr, sizes2);
        fail("Exception should have been thrown.");
      } catch (IllegalArgumentException e) {}

      try {
        int[] sizes2 = {Separator.VARIABLE_SIZE, Separator.VARIABLE_SIZE, 2,
            Bytes.SIZEOF_LONG};
        splits = separator.split(arr, sizes2);
        fail("Exception should have been thrown.");
      } catch (IllegalArgumentException e) {}
    }
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

    String[] stringArray1 = {"else"};
    joined = Separator.VALUES.joinEncoded(stringArray1);
    split = Separator.VALUES.splitEncoded(joined);
    assertTrue(Iterables.elementsEqual(Arrays.asList(stringArray1), split));

    String[] stringArray2 = {"d", "e?", "f"};
    joined = Separator.VALUES.joinEncoded(stringArray2);
    split = Separator.VALUES.splitEncoded(joined);
    assertTrue(Iterables.elementsEqual(Arrays.asList(stringArray2), split));

    List<String> empty = new ArrayList<String>(0);
    split = Separator.VALUES.splitEncoded(null);
    assertTrue(Iterables.elementsEqual(empty, split));

  }

}
