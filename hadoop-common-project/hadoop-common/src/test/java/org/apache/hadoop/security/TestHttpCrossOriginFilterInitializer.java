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

package org.apache.hadoop.security;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import org.junit.Assert;
import org.junit.Test;

public class TestHttpCrossOriginFilterInitializer {

  @Test
  public void testGetFilterParameters() {

    // Initialize configuration object
    Configuration conf = new Configuration();
    conf.set(HttpCrossOriginFilterInitializer.PREFIX + "rootparam",
        "rootvalue");
    conf.set(HttpCrossOriginFilterInitializer.PREFIX + "nested.param",
        "nestedvalue");
    conf.set("outofscopeparam", "outofscopevalue");

    // call function under test
    Map<String, String> filterParameters = HttpCrossOriginFilterInitializer
        .getFilterParameters(conf, HttpCrossOriginFilterInitializer.PREFIX);

    // retrieve values
    String rootvalue = filterParameters.get("rootparam");
    String nestedvalue = filterParameters.get("nested.param");
    String outofscopeparam = filterParameters.get("outofscopeparam");

    // verify expected values are in place
    Assert.assertEquals("Could not find filter parameter", "rootvalue",
        rootvalue);
    Assert.assertEquals("Could not find filter parameter", "nestedvalue",
        nestedvalue);
    Assert.assertNull("Found unexpected value in filter parameters",
        outofscopeparam);
  }
}
