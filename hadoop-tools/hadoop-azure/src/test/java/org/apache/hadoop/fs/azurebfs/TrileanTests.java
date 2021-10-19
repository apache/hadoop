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

package org.apache.hadoop.fs.azurebfs;

import org.junit.Test;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.TrileanConversionException;
import org.apache.hadoop.fs.azurebfs.enums.Trilean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Tests for the enum Trilean.
 */
public class TrileanTests {

  private static final String TRUE_STR = "true";
  private static final String FALSE_STR = "false";

  @Test
  public void testGetTrileanForBoolean() {
    assertThat(Trilean.getTrilean(true)).describedAs(
        "getTrilean should return Trilean.TRUE when true is passed")
        .isEqualTo(Trilean.TRUE);
    assertThat(Trilean.getTrilean(false)).describedAs(
        "getTrilean should return Trilean.FALSE when false is passed")
        .isEqualTo(Trilean.FALSE);
  }

  @Test
  public void testGetTrileanForString() {
    assertThat(Trilean.getTrilean(TRUE_STR.toLowerCase())).describedAs(
        "getTrilean should return Trilean.TRUE when true is passed")
        .isEqualTo(Trilean.TRUE);
    assertThat(Trilean.getTrilean(TRUE_STR.toUpperCase())).describedAs(
        "getTrilean should return Trilean.TRUE when TRUE is passed")
        .isEqualTo(Trilean.TRUE);

    assertThat(Trilean.getTrilean(FALSE_STR.toLowerCase())).describedAs(
        "getTrilean should return Trilean.FALSE when false is passed")
        .isEqualTo(Trilean.FALSE);
    assertThat(Trilean.getTrilean(FALSE_STR.toUpperCase())).describedAs(
        "getTrilean should return Trilean.FALSE when FALSE is passed")
        .isEqualTo(Trilean.FALSE);

    testInvalidString(null);
    testInvalidString(" ");
    testInvalidString("invalid");
    testInvalidString("truee");
    testInvalidString("falsee");
  }

  private void testInvalidString(String invalidString) {
    assertThat(Trilean.getTrilean(invalidString)).describedAs(
        "getTrilean should return Trilean.UNKNOWN for anything not true/false")
        .isEqualTo(Trilean.UNKNOWN);
  }

  @Test
  public void testToBoolean() throws TrileanConversionException {
    assertThat(Trilean.TRUE.toBoolean())
        .describedAs("toBoolean should return true for Trilean.TRUE").isTrue();
    assertThat(Trilean.FALSE.toBoolean())
        .describedAs("toBoolean should return false for Trilean.FALSE")
        .isFalse();

    assertThat(catchThrowable(() -> Trilean.UNKNOWN.toBoolean())).describedAs(
        "toBoolean on Trilean.UNKNOWN results in TrileanConversionException")
        .isInstanceOf(TrileanConversionException.class).describedAs(
        "Exception message should be: catchThrowable(()->Trilean.UNKNOWN"
            + ".toBoolean())")
        .hasMessage("Cannot convert Trilean.UNKNOWN to boolean");
  }

}
