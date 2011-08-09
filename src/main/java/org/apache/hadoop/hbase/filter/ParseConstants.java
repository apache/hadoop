/**
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.filter;

import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;
import java.nio.ByteBuffer;
import java.util.HashMap;
import org.apache.hadoop.hbase.filter.*;

/**
 * ParseConstants holds a bunch of constants related to parsing Filter Strings
 * Used by {@link ParseFilter}
 */
public final class ParseConstants {

  /**
   * ASCII code for LPAREN
   */
  public static final int LPAREN = '(';

  /**
   * ASCII code for RPAREN
   */
  public static final int RPAREN = ')';

  /**
   * ASCII code for whitespace
   */
  public static final int WHITESPACE = ' ';

  /**
   * ASCII code for tab
   */
  public static final int TAB = '\t';

  /**
   * ASCII code for 'A'
   */
  public static final int A = 'A';

  /**
   * ASCII code for 'N'
   */
  public static final int N = 'N';

  /**
   * ASCII code for 'D'
   */
  public static final int D = 'D';

  /**
   * ASCII code for 'O'
   */
  public static final int O = 'O';

  /**
   * ASCII code for 'R'
   */
  public static final int R = 'R';

  /**
   * ASCII code for 'S'
   */
  public static final int S = 'S';

  /**
   * ASCII code for 'K'
   */
  public static final int K = 'K';

  /**
   * ASCII code for 'I'
   */
  public static final int I = 'I';

  /**
   * ASCII code for 'P'
   */
  public static final int P = 'P';

  /**
   * SKIP Array
   */
  public static final byte [] SKIP_ARRAY = new byte [ ] {'S', 'K', 'I', 'P'};
  public static final ByteBuffer SKIP_BUFFER = ByteBuffer.wrap(SKIP_ARRAY);

  /**
   * ASCII code for 'W'
   */
  public static final int W = 'W';

  /**
   * ASCII code for 'H'
   */
  public static final int H = 'H';

  /**
   * ASCII code for 'L'
   */
  public static final int L = 'L';

  /**
   * ASCII code for 'E'
   */
  public static final int E = 'E';

  /**
   * WHILE Array
   */
  public static final byte [] WHILE_ARRAY = new byte [] {'W', 'H', 'I', 'L', 'E'};
  public static final ByteBuffer WHILE_BUFFER = ByteBuffer.wrap(WHILE_ARRAY);

  /**
   * OR Array
   */
  public static final byte [] OR_ARRAY = new byte [] {'O','R'};
  public static final ByteBuffer OR_BUFFER = ByteBuffer.wrap(OR_ARRAY);

  /**
   * AND Array
   */
  public static final byte [] AND_ARRAY = new byte [] {'A','N', 'D'};
  public static final ByteBuffer AND_BUFFER = ByteBuffer.wrap(AND_ARRAY);

  /**
   * ASCII code for Backslash
   */
  public static final int BACKSLASH = '\\';

  /**
   * ASCII code for a single quote
   */
  public static final int SINGLE_QUOTE = '\'';

  /**
   * ASCII code for a comma
   */
  public static final int COMMA = ',';

  /**
   * LESS_THAN Array
   */
  public static final byte [] LESS_THAN_ARRAY = new byte [] {'<'};
  public static final ByteBuffer LESS_THAN_BUFFER = ByteBuffer.wrap(LESS_THAN_ARRAY);

  /**
   * LESS_THAN_OR_EQUAL_TO Array
   */
  public static final byte [] LESS_THAN_OR_EQUAL_TO_ARRAY = new byte [] {'<', '='};
  public static final ByteBuffer LESS_THAN_OR_EQUAL_TO_BUFFER =
    ByteBuffer.wrap(LESS_THAN_OR_EQUAL_TO_ARRAY);

  /**
   * GREATER_THAN Array
   */
  public static final byte [] GREATER_THAN_ARRAY = new byte [] {'>'};
  public static final ByteBuffer GREATER_THAN_BUFFER = ByteBuffer.wrap(GREATER_THAN_ARRAY);

  /**
   * GREATER_THAN_OR_EQUAL_TO Array
   */
  public static final byte [] GREATER_THAN_OR_EQUAL_TO_ARRAY = new byte [] {'>', '='};
  public static final ByteBuffer GREATER_THAN_OR_EQUAL_TO_BUFFER =
    ByteBuffer.wrap(GREATER_THAN_OR_EQUAL_TO_ARRAY);

  /**
   * EQUAL_TO Array
   */
  public static final byte [] EQUAL_TO_ARRAY = new byte [] {'='};
  public static final ByteBuffer EQUAL_TO_BUFFER = ByteBuffer.wrap(EQUAL_TO_ARRAY);

  /**
   * NOT_EQUAL_TO Array
   */
  public static final byte [] NOT_EQUAL_TO_ARRAY = new byte [] {'!', '='};
  public static final ByteBuffer NOT_EQUAL_TO_BUFFER = ByteBuffer.wrap(NOT_EQUAL_TO_ARRAY);

  /**
   * ASCII code for equal to (=)
   */
  public static final int EQUAL_TO = '=';

  /**
   * AND Byte Array
   */
  public static final byte [] AND = new byte [] {'A','N','D'};

  /**
   * OR Byte Array
   */
  public static final byte [] OR = new byte [] {'O', 'R'};

  /**
   * LPAREN Array
   */
  public static final byte [] LPAREN_ARRAY = new byte [] {'('};
  public static final ByteBuffer LPAREN_BUFFER = ByteBuffer.wrap(LPAREN_ARRAY);

  /**
   * ASCII code for colon (:)
   */
  public static final int COLON = ':';

  /**
   * ASCII code for Zero
   */
  public static final int ZERO = '0';

  /**
   * ASCII code foe Nine
   */
  public static final int NINE = '9';

  /**
   * BinaryType byte array
   */
  public static final byte [] binaryType = new byte [] {'b','i','n','a','r','y'};

  /**
   * BinaryPrefixType byte array
   */
  public static final byte [] binaryPrefixType = new byte [] {'b','i','n','a','r','y',
                                                              'p','r','e','f','i','x'};

  /**
   * RegexStringType byte array
   */
  public static final byte [] regexStringType = new byte [] {'r','e','g','e', 'x',
                                                             's','t','r','i','n','g'};

  /**
   * SubstringType byte array
   */
  public static final byte [] substringType = new byte [] {'s','u','b','s','t','r','i','n','g'};

  /**
   * ASCII for Minus Sign
   */
  public static final int MINUS_SIGN = '-';
}