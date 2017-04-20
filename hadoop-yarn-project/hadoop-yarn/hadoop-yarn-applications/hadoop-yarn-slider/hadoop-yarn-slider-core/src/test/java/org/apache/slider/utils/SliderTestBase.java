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

package org.apache.slider.utils;

import org.apache.hadoop.fs.FileUtil;
import org.apache.slider.common.SliderXMLConfKeysForTesting;
import org.apache.slider.server.appmaster.management.MetricsAndMonitoring;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.File;


/**
 * Base class for unit tests as well as ones starting mini clusters
 * -the foundational code and methods.
 *
 */
public abstract class SliderTestBase extends SliderTestUtils {

  /**
   * Singleton metric registry.
   */
  public static final MetricsAndMonitoring METRICS = new MetricsAndMonitoring();
  public static final int WEB_STARTUP_TIME = 30000;

  @Rule
  public TestName methodName = new TestName();

  @BeforeClass
  public static void nameThread() {
    Thread.currentThread().setName("JUnit");
  }

  @Before
  public void setup() throws Exception {
    setSliderClientClassName(DEFAULT_SLIDER_CLIENT);
    FileUtil.fullyDelete(new File(SliderXMLConfKeysForTesting
        .TEST_SECURITY_DIR));
  }

}
