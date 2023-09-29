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
package org.apache.hadoop.tools.fedbalance.procedure;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This simulates a procedure needs many retries. This is used for test.
 */
public class RetryProcedure extends BalanceProcedure {

  private int retryTime = 1;
  private int totalRetry = 0;

  public RetryProcedure() {}

  public RetryProcedure(String name, long delay, int retryTime) {
    super(name, delay);
    this.retryTime = retryTime;
  }

  @Override
  public boolean execute() throws RetryException {
    if (retryTime > 0) {
      retryTime--;
      totalRetry++;
      throw new RetryException();
    }
    return true;
  }

  public int getTotalRetry() {
    return totalRetry;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(retryTime);
    out.writeInt(totalRetry);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    retryTime = in.readInt();
    totalRetry = in.readInt();
  }
}
