/*
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

package org.apache.slider.common.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.slider.utils.SliderTestUtils;
import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;

/**
 * Test slider test utils.
 */
public class TestSliderTestUtils extends SliderTestUtils {

  //@Test
  public void testAssumeTrue() throws Throwable {

    try {
      assume(true, "true");
    } catch (AssumptionViolatedException e) {
      throw new Exception(e);
    }
  }

  //@Test
  public void testAssumeFalse() throws Throwable {

    try {
      assume(false, "false");
      fail("expected an exception");
    } catch (AssumptionViolatedException ignored) {
      //expected
    }
  }

  //@Test
  public void testAssumeBoolOptionSetInConf() throws Throwable {
    Configuration conf = new Configuration(false);
    conf.set("key", "true");
    try {
      assumeBoolOption(conf, "key", false);
    } catch (AssumptionViolatedException e) {
      throw new Exception(e);
    }
  }

  //@Test
  public void testAssumeBoolOptionUnsetInConf() throws Throwable {
    Configuration conf = new Configuration(false);
    try {
      assumeBoolOption(conf, "key", true);
    } catch (AssumptionViolatedException e) {
      throw new Exception(e);
    }
  }


  //@Test
  public void testAssumeBoolOptionFalseInConf() throws Throwable {
    Configuration conf = new Configuration(false);
    conf.set("key", "false");
    try {
      assumeBoolOption(conf, "key", true);
      fail("expected an exception");
    } catch (AssumptionViolatedException ignored) {
      //expected
    }
  }

  //@Test
  public void testAssumeBoolOptionFalseUnsetInConf() throws Throwable {
    Configuration conf = new Configuration(false);
    try {
      assumeBoolOption(conf, "key", false);
      fail("expected an exception");
    } catch (AssumptionViolatedException ignored) {
      //expected
    }
  }

}
