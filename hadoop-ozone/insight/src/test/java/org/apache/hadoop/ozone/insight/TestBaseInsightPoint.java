/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.insight;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 * Test common insight point utility methods.
 */
public class TestBaseInsightPoint {

  @Test
  public void filterLog() {

    BaseInsightPoint insightPoint = new BaseInsightPoint() {
      @Override
      public String getDescription() {
        return "test";
      }
    };

    //with simple filter
    Map<String, String> filters = new HashMap<>();
    filters.put("datanode", "123");

    Assert.assertTrue(insightPoint
        .filterLog(filters, "This a log specific to [datanode=123]"));

    Assert.assertFalse(insightPoint
        .filterLog(filters, "This a log specific to [datanode=234]"));

    //with empty filters
    Assert.assertTrue(insightPoint
        .filterLog(new HashMap<>(), "This a log specific to [datanode=234]"));

    //with multiple filters
    filters.clear();
    filters.put("datanode", "123");
    filters.put("pipeline", "abcd");

    Assert.assertFalse(insightPoint
        .filterLog(filters, "This a log specific to [datanode=123]"));

    Assert.assertTrue(insightPoint
        .filterLog(filters,
            "This a log specific to [datanode=123] [pipeline=abcd]"));

    Assert.assertFalse(insightPoint
        .filterLog(filters,
            "This a log specific to [datanode=456] [pipeline=abcd]"));

  }
}