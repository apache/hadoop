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
package org.apache.hadoop.yarn.server.nodemanager.webapp;

import org.junit.jupiter.api.Test;

import static org.apache.hadoop.test.ClasspathTestUtils.assertSingleJettyRelease;
import static org.apache.hadoop.test.ClasspathTestUtils.assertSingleProvider;
import static org.apache.hadoop.test.ClasspathTestUtils.assertSingleServletApi;

/**
 * HADOOP-19970: this module's classpath resolves one servlet API and one
 * Jetty release.
 */
public class TestJettyClasspath {

  @Test
  public void testSingleServletApi() throws Exception {
    assertSingleServletApi();
  }

  @Test
  public void testSingleJettyRelease() throws Exception {
    assertSingleJettyRelease();
  }

  /**
   * jetty-annotations brings javax.annotation-api, a second copy of the
   * javax.annotation classes in jakarta.annotation-api.
   */
  @Test
  public void testSingleAnnotationApi() throws Exception {
    assertSingleProvider("javax/annotation/PostConstruct.class",
        "/jakarta.annotation-api-");
  }
}
