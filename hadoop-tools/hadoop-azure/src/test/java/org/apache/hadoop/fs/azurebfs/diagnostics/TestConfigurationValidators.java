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

package org.apache.hadoop.fs.azurebfs.diagnostics;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidConfigurationValueException;
import org.apache.hadoop.fs.azurebfs.utils.Base64;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_LEASE_DURATION;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_READ_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_WRITE_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.INFINITE_LEASE_DURATION;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MAX_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MAX_LEASE_DURATION;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MIN_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MIN_LEASE_DURATION;

/**
 * Test configuration validators.
 */
public class TestConfigurationValidators extends Assert {

  private static final String FAKE_KEY = "FakeKey";

  public TestConfigurationValidators() throws Exception {
    super();
  }

  @Test
  public void testIntegerConfigValidator() throws Exception {
    IntegerConfigurationBasicValidator integerConfigurationValidator = new IntegerConfigurationBasicValidator(
        MIN_BUFFER_SIZE, MAX_BUFFER_SIZE, DEFAULT_READ_BUFFER_SIZE, FAKE_KEY, false);

    assertEquals(MIN_BUFFER_SIZE, (int) integerConfigurationValidator.validate("3072"));
    assertEquals(DEFAULT_READ_BUFFER_SIZE, (int) integerConfigurationValidator.validate(null));
    assertEquals(MAX_BUFFER_SIZE, (int) integerConfigurationValidator.validate("104857600"));
  }

  @Test(expected = InvalidConfigurationValueException.class)
  public void testIntegerConfigValidatorThrowsIfMissingValidValue() throws Exception {
    IntegerConfigurationBasicValidator integerConfigurationValidator = new IntegerConfigurationBasicValidator(
        MIN_BUFFER_SIZE, MAX_BUFFER_SIZE, DEFAULT_READ_BUFFER_SIZE, FAKE_KEY, true);
    integerConfigurationValidator.validate("3072");
  }

  @Test
  public void testIntegerWithOutlierConfigValidator() throws Exception {
    IntegerConfigurationBasicValidator integerConfigurationValidator = new IntegerConfigurationBasicValidator(
        INFINITE_LEASE_DURATION, MIN_LEASE_DURATION, MAX_LEASE_DURATION, DEFAULT_LEASE_DURATION, FAKE_KEY,
        false);

    assertEquals(INFINITE_LEASE_DURATION, (int) integerConfigurationValidator.validate("-1"));
    assertEquals(DEFAULT_LEASE_DURATION, (int) integerConfigurationValidator.validate(null));
    assertEquals(MIN_LEASE_DURATION, (int) integerConfigurationValidator.validate("15"));
    assertEquals(MAX_LEASE_DURATION, (int) integerConfigurationValidator.validate("60"));
  }

  @Test(expected = InvalidConfigurationValueException.class)
  public void testIntegerWithOutlierConfigValidatorThrowsIfMissingValidValue() throws Exception {
    IntegerConfigurationBasicValidator integerConfigurationValidator = new IntegerConfigurationBasicValidator(
        INFINITE_LEASE_DURATION, MIN_LEASE_DURATION, MAX_LEASE_DURATION, DEFAULT_LEASE_DURATION, FAKE_KEY,
        true);
    integerConfigurationValidator.validate("14");
  }

  @Test
  public void testLongConfigValidator() throws Exception {
    LongConfigurationBasicValidator longConfigurationValidator = new LongConfigurationBasicValidator(
        MIN_BUFFER_SIZE, MAX_BUFFER_SIZE, DEFAULT_WRITE_BUFFER_SIZE, FAKE_KEY, false);

    assertEquals(DEFAULT_WRITE_BUFFER_SIZE, (long) longConfigurationValidator.validate(null));
    assertEquals(MIN_BUFFER_SIZE, (long) longConfigurationValidator.validate("3072"));
    assertEquals(MAX_BUFFER_SIZE, (long) longConfigurationValidator.validate("104857600"));
  }

  @Test(expected = InvalidConfigurationValueException.class)
  public void testLongConfigValidatorThrowsIfMissingValidValue() throws Exception {
    LongConfigurationBasicValidator longConfigurationValidator = new LongConfigurationBasicValidator(
        MIN_BUFFER_SIZE, MAX_BUFFER_SIZE, DEFAULT_READ_BUFFER_SIZE, FAKE_KEY, true);
    longConfigurationValidator.validate(null);
  }

  @Test
  public void testBooleanConfigValidator() throws Exception {
    BooleanConfigurationBasicValidator booleanConfigurationValidator = new BooleanConfigurationBasicValidator(FAKE_KEY, false, false);

    assertEquals(true, booleanConfigurationValidator.validate("true"));
    assertEquals(false, booleanConfigurationValidator.validate("False"));
    assertEquals(false, booleanConfigurationValidator.validate(null));
  }

  @Test(expected = InvalidConfigurationValueException.class)
  public void testBooleanConfigValidatorThrowsIfMissingValidValue() throws Exception {
    BooleanConfigurationBasicValidator booleanConfigurationValidator = new BooleanConfigurationBasicValidator(FAKE_KEY, false, true);
    booleanConfigurationValidator.validate("almostTrue");
  }

  @Test
  public void testStringConfigValidator() throws Exception {
    StringConfigurationBasicValidator stringConfigurationValidator = new StringConfigurationBasicValidator(FAKE_KEY, "value", false);

    assertEquals("value", stringConfigurationValidator.validate(null));
    assertEquals("someValue", stringConfigurationValidator.validate("someValue"));
  }

  @Test(expected = InvalidConfigurationValueException.class)
  public void testStringConfigValidatorThrowsIfMissingValidValue() throws Exception {
    StringConfigurationBasicValidator stringConfigurationValidator = new StringConfigurationBasicValidator(FAKE_KEY, "value", true);
    stringConfigurationValidator.validate(null);
  }

  @Test
  public void testBase64StringConfigValidator() throws Exception {
    String encodedVal = Base64.encode("someValue".getBytes());
    Base64StringConfigurationBasicValidator base64StringConfigurationValidator = new Base64StringConfigurationBasicValidator(FAKE_KEY, "", false);

    assertEquals("", base64StringConfigurationValidator.validate(null));
    assertEquals(encodedVal, base64StringConfigurationValidator.validate(encodedVal));
  }

  @Test(expected = InvalidConfigurationValueException.class)
  public void testBase64StringConfigValidatorThrowsIfMissingValidValue() throws Exception {
    Base64StringConfigurationBasicValidator base64StringConfigurationValidator = new Base64StringConfigurationBasicValidator(FAKE_KEY, "value", true);
    base64StringConfigurationValidator.validate("some&%Value");
  }
}
