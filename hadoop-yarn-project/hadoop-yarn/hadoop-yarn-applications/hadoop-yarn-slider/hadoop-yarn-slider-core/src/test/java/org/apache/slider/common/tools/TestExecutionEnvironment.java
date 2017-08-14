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

import org.apache.slider.utils.SliderTestBase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test execution environment.
 */
public class TestExecutionEnvironment extends SliderTestBase {
  protected static final Logger LOG =
      LoggerFactory.getLogger(TestExecutionEnvironment.class);

  //@Test
  public void testClientEnv() throws Throwable {
    SliderUtils.validateSliderClientEnvironment(LOG);
  }

  //@Test
  public void testWinutils() throws Throwable {
    SliderUtils.maybeVerifyWinUtilsValid();
  }

  //@Test
  public void testServerEnv() throws Throwable {
    SliderUtils.validateSliderServerEnvironment(LOG, true);
  }

  //@Test
  public void testServerEnvNoDependencies() throws Throwable {
    SliderUtils.validateSliderServerEnvironment(LOG, false);
  }

  //@Test
  public void testopenSSLEnv() throws Throwable {
    SliderUtils.validateOpenSSLEnv(LOG);
  }

  //@Test
  public void testValidatePythonEnv() throws Throwable {
    SliderUtils.validatePythonEnv(LOG);
  }

  //@Test
  public void testNativeLibs() throws Throwable {
    assertNativeLibrariesPresent();
  }
}
