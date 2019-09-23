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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.ozone.s3.util;

import java.time.temporal.TemporalAccessor;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for RFC1123 util.
 */
public class TestRFC1123Util {

  @Test
  public void parse() {
    //one digit day
    String dateStr = "Mon, 5 Nov 2018 15:04:05 GMT";

    TemporalAccessor date = RFC1123Util.FORMAT.parse(dateStr);

    String formatted = RFC1123Util.FORMAT.format(date);

    //two digits day
    Assert.assertEquals("Mon, 05 Nov 2018 15:04:05 GMT", formatted);

  }
}