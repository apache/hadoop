/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.enums;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.TrileanConversionException;

/**
 * Enum to represent 3 values, TRUE, FALSE and UNKNOWN. Can be used where
 * boolean is not enough to hold the information.
 */
public enum Trilean {

  FALSE, TRUE, UNKNOWN;

  private static final String TRUE_STR = "true";
  private static final String FALSE_STR = "false";

  /**
   * Converts boolean to Trilean.
   *
   * @param isTrue the boolean to convert.
   * @return the corresponding Trilean for the passed boolean isTrue.
   */
  public static Trilean getTrilean(final boolean isTrue) {
    if (isTrue) {
      return Trilean.TRUE;
    }

    return Trilean.FALSE;
  }

  /**
   * Converts String to Trilean.
   *
   * @param str the string to convert.
   * @return the corresponding Trilean for the passed string str.
   */
  public static Trilean getTrilean(String str) {
    if (TRUE_STR.equalsIgnoreCase(str)) {
      return Trilean.TRUE;
    }

    if (FALSE_STR.equalsIgnoreCase(str)) {
      return Trilean.FALSE;
    }

    return Trilean.UNKNOWN;
  }

  /**
   * Converts the Trilean enum to boolean.
   *
   * @return the corresponding boolean.
   * @throws TrileanConversionException when tried to convert Trilean.UNKNOWN.
   */
  public boolean toBoolean() throws TrileanConversionException {
    if (this == Trilean.UNKNOWN) {
      throw new TrileanConversionException();
    }

    return Boolean.valueOf(this.name());
  }

}
