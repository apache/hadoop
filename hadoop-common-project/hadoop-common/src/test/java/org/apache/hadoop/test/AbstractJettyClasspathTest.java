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
package org.apache.hadoop.test;

import org.junit.jupiter.api.Test;

import static org.apache.hadoop.test.ClasspathTestUtils.assertSingleJettyRelease;
import static org.apache.hadoop.test.ClasspathTestUtils.assertSingleServletApi;

/**
 * HADOOP-19970: the test classpath of a module that runs Hadoop's web
 * servers resolves one servlet API and one Jetty release. A module checks
 * its own classpath with an empty subclass named TestJettyClasspath; JUnit
 * runs the inherited tests, so every module checks the same things.
 */
public abstract class AbstractJettyClasspathTest {

  @Test
  public void testSingleServletApi() throws Exception {
    assertSingleServletApi();
  }

  @Test
  public void testSingleJettyRelease() throws Exception {
    assertSingleJettyRelease();
  }
}
