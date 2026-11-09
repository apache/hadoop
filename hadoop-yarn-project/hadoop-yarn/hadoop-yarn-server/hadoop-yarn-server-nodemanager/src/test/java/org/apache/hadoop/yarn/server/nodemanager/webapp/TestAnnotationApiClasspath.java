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

import java.net.URL;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * HADOOP-20013: the NodeManager is the module that declares
 * javax-websocket-server-impl, whose jetty-annotations brings
 * javax.annotation-api, a second copy of the javax.annotation classes that
 * jakarta.annotation-api already provides. Its test classpath keeps one.
 */
public class TestAnnotationApiClasspath {

  @Test
  public void testSingleAnnotationApi() throws Exception {
    String resource = "javax/annotation/PostConstruct.class";
    List<URL> providers = Collections.list(
        getClass().getClassLoader().getResources(resource));
    assertEquals(1, providers.size(), "providers of " + resource + ": "
        + providers);
    assertTrue(providers.get(0).toString().contains("/jakarta.annotation-api-"),
        "unexpected provider of " + resource + ": " + providers.get(0));
  }
}
