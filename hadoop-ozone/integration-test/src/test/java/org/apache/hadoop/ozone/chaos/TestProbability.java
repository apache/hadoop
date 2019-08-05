/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.chaos;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.RandomUtils;

/**
 * This class is used to find out if a certain event is true.
 * Every event is assigned a propbability and the isTrue function returns true
 * when the probability has been met.
 */
final public class TestProbability {
  private int pct;

  private TestProbability(int pct) {
    Preconditions.checkArgument(pct <= 100 && pct > 0);
    this.pct = pct;
  }

  public boolean isTrue() {
    return (RandomUtils.nextInt(0, 100) <= pct);
  }

  public static TestProbability valueOf(int pct) {
    return new TestProbability(pct);
  }
}
