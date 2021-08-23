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
package org.apache.hadoop.hdfs.web;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestJsonUtilClient {
  @Test
  public void testToStringArray() throws Exception {
    List<String> strList = new ArrayList<String>(
        Arrays.asList("aaa", "bbb", "ccc"));

    String[] strArr = JsonUtilClient.toStringArray(strList);
    Assert.assertEquals("Expected 3 items in the array", 3, strArr.length);
    Assert.assertEquals("aaa", strArr[0]);
    Assert.assertEquals("bbb", strArr[1]);
    Assert.assertEquals("ccc", strArr[2]);
  }
}
