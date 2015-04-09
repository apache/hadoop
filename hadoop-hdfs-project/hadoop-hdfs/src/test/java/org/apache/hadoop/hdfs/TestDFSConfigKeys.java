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
package org.apache.hadoop.hdfs;

import org.apache.hadoop.hdfs.web.AuthFilter;

import org.junit.Assert;
import org.junit.Test;

public class TestDFSConfigKeys {

  /**
   * Make sure we keep the String literal up to date with what we'd get by calling
   * class.getName.
   */
  @Test
  public void testStringLiteralDefaultWebFilter() {
    Assert.assertEquals("The default webhdfs auth filter should make the FQCN of AuthFilter.",
        AuthFilter.class.getName(), DFSConfigKeys.DFS_WEBHDFS_AUTHENTICATION_FILTER_DEFAULT);
  }
 
}
