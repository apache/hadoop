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
package org.apache.hadoop.hdfs.server.namenode.top.window;

import org.junit.Assert;
import org.junit.Test;

public class TestRollingWindow {

  final int WINDOW_LEN = 60000;
  final int BUCKET_CNT = 10;
  final int BUCKET_LEN = WINDOW_LEN / BUCKET_CNT;

  @Test
  public void testBasics() {
    RollingWindow window = new RollingWindow(WINDOW_LEN, BUCKET_CNT);
    long time = 1;
    Assert.assertEquals("The initial sum of rolling window must be 0", 0,
        window.getSum(time));
    time = WINDOW_LEN + BUCKET_LEN * 3 / 2;
    Assert.assertEquals("The initial sum of rolling window must be 0", 0,
        window.getSum(time));

    window.incAt(time, 5);
    Assert.assertEquals(
        "The sum of rolling window does not reflect the recent update", 5,
        window.getSum(time));

    time += BUCKET_LEN;
    window.incAt(time, 6);
    Assert.assertEquals(
        "The sum of rolling window does not reflect the recent update", 11,
        window.getSum(time));

    time += WINDOW_LEN - BUCKET_LEN;
    Assert.assertEquals(
        "The sum of rolling window does not reflect rolling effect", 6,
        window.getSum(time));

    time += BUCKET_LEN;
    Assert.assertEquals(
        "The sum of rolling window does not reflect rolling effect", 0,
        window.getSum(time));
  }

  @Test
  public void testReorderedAccess() {
    RollingWindow window = new RollingWindow(WINDOW_LEN, BUCKET_CNT);
    long time = 2 * WINDOW_LEN + BUCKET_LEN * 3 / 2;
    window.incAt(time, 5);

    time++;
    Assert.assertEquals(
        "The sum of rolling window does not reflect the recent update", 5,
        window.getSum(time));

    long reorderedTime = time - 2 * BUCKET_LEN;
    window.incAt(reorderedTime, 6);
    Assert.assertEquals(
        "The sum of rolling window does not reflect the reordered update", 11,
        window.getSum(time));

    time = reorderedTime + WINDOW_LEN;
    Assert.assertEquals(
        "The sum of rolling window does not reflect rolling effect", 5,
        window.getSum(time));
  }

}
