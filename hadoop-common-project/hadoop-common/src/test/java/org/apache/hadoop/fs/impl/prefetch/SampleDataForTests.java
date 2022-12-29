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

package org.apache.hadoop.fs.impl.prefetch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Frequently used test data items.
 */
public final class SampleDataForTests {

  private SampleDataForTests() {
  }


  // Array data.
  public static final Object[] NULL_ARRAY = null;

  public static final Object[] EMPTY_ARRAY = new Object[0];

  public static final Object[] NON_EMPTY_ARRAY = new Object[1];

  public static final byte[] NULL_BYTE_ARRAY = null;

  public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  public static final byte[] NON_EMPTY_BYTE_ARRAY = new byte[1];

  public static final short[] NULL_SHORT_ARRAY = null;

  public static final short[] EMPTY_SHORT_ARRAY = new short[0];

  public static final short[] NON_EMPTY_SHORT_ARRAY = new short[1];

  public static final int[] NULL_INT_ARRAY = null;

  public static final int[] EMPTY_INT_ARRAY = new int[0];

  public static final int[] NON_EMPTY_INT_ARRAY = new int[1];

  public static final long[] NULL_LONG_ARRAY = null;

  public static final long[] EMPTY_LONG_ARRAY = new long[0];

  public static final long[] NON_EMPTY_LONG_ARRAY = new long[1];

  public static final List<Object> NULL_LIST = null;

  public static final List<Object> EMPTY_LIST = new ArrayList<Object>();

  public static final List<Object> VALID_LIST = Arrays.asList(new Object[1]);
}
