/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Frequently used test data items.
 */
public final class TestData {
  private TestData() {}


  // Array data.
  public static Object[] nullArray     = null;
  public static Object[] emptyArray    = new Object[0];
  public static Object[] nonEmptyArray = new Object[1];

  public static byte[] nullByteArray       = null;
  public static byte[] emptyByteArray      = new byte[0];
  public static byte[] nonEmptyByteArray   = new byte[1];

  public static short[] nullShortArray     = null;
  public static short[] emptyShortArray    = new short[0];
  public static short[] nonEmptyShortArray = new short[1];

  public static int[] nullIntArray         = null;
  public static int[] emptyIntArray        = new int[0];
  public static int[] nonEmptyIntArray     = new int[1];

  public static long[] nullLongArray       = null;
  public static long[] emptyLongArray      = new long[0];
  public static long[] nonEmptyLongArray   = new long[1];

  public static List<Object> nullList  = null;
  public static List<Object> emptyList = new ArrayList<Object>();
  public static List<Object> validList = Arrays.asList(new Object[1]);
}
