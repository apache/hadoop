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

package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDAF;



public class UDAFAvg extends UDAF {

  private long mCount;
  private double mSum;
  
  public UDAFAvg() {
    super();
    init();
  }

  public void init() {
    mSum = 0;
    mCount = 0;
  }

  public boolean aggregate(String o) {
    if (o != null && !o.isEmpty()) {
      mSum += Double.parseDouble(o);
      mCount ++;
    }
    return true;
  }
  
  public String evaluatePartial() {
    // This is SQL standard - average of zero items should be null.
    return mCount == 0 ? null : String.valueOf(mSum) + '/' + String.valueOf(mCount);
  }

  public boolean aggregatePartial(String o) {
    if (o != null && !o.isEmpty()) {
      int pos = o.indexOf('/');
      assert(pos != -1);
      mSum += Double.parseDouble(o.substring(0, pos));
      mCount += Long.parseLong(o.substring(pos+1));
    }
    return true;
  }

  public String evaluate() {
    // This is SQL standard - average of zero items should be null.
    return mCount == 0 ? null : String.valueOf(mSum / mCount);
  }

}
