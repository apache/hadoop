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
package org.apache.hadoop.hdfs.server.federation.resolver;

import org.junit.Test;

import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

public class TestFederationNamespaceInfo {
  /**
   * Regression test for HDFS-15900.
   */
  @Test
  public void testHashCode() {
    Set<FederationNamespaceInfo> set = new TreeSet<>();
    // set an empty bpId first
    set.add(new FederationNamespaceInfo("", "nn1", "ns1"));
    set.add(new FederationNamespaceInfo("bp1", "nn2", "ns1"));
    assertThat(set).hasSize(2);
  }
}
