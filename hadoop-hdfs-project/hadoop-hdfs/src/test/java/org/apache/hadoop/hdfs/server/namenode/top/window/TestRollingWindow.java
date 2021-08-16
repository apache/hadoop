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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRollingWindow {

  final int WINDOW_LEN = 60000;
  final int BUCKET_CNT = 10;
  final int BUCKET_LEN = WINDOW_LEN / BUCKET_CNT;

  @Test
  public void testBasics() {
    RollingWindow window = new RollingWindow(WINDOW_LEN, BUCKET_CNT);
    long time = 1;
      Assertions.assertEquals(0,
              window.getSum(time), "The initial sum of rolling window must be 0");
    time = WINDOW_LEN + BUCKET_LEN * 3 / 2;
      Assertions.assertEquals(0,
              window.getSum(time), "The initial sum of rolling window must be 0");

    window.incAt(time, 5);
      Assertions.assertEquals(5,
              window.getSum(time),
              "The sum of rolling window does not reflect the recent update");

    time += BUCKET_LEN;
    window.incAt(time, 6);
      Assertions.assertEquals(11,
              window.getSum(time),
              "The sum of rolling window does not reflect the recent update");

    time += WINDOW_LEN - BUCKET_LEN;
      Assertions.assertEquals(6,
              window.getSum(time),
              "The sum of rolling window does not reflect rolling effect");

    time += BUCKET_LEN;
      Assertions.assertEquals(0,
              window.getSum(time),
              "The sum of rolling window does not reflect rolling effect");
  }

  @Test
  public void testReorderedAccess() {
    RollingWindow window = new RollingWindow(WINDOW_LEN, BUCKET_CNT);
    long time = 2 * WINDOW_LEN + BUCKET_LEN * 3 / 2;
    window.incAt(time, 5);

    time++;
      Assertions.assertEquals(5,
              window.getSum(time),
              "The sum of rolling window does not reflect the recent update");

    long reorderedTime = time - 2 * BUCKET_LEN;
    window.incAt(reorderedTime, 6);
      Assertions.assertEquals(11,
              window.getSum(time),
              "The sum of rolling window does not reflect the reordered update");

    time = reorderedTime + WINDOW_LEN;
      Assertions.assertEquals(5,
              window.getSum(time),
              "The sum of rolling window does not reflect rolling effect");
  }

}
