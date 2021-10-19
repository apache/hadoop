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
package org.apache.hadoop.hdfs.rbfbalance;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.fedbalance.DistCpProcedure;
import org.apache.hadoop.tools.fedbalance.FedBalanceContext;

import java.io.IOException;

/**
 * Copy data through distcp in router-based federation cluster. It disables
 * write by setting mount entry readonly.
 */
public class RouterDistCpProcedure extends DistCpProcedure {

  public RouterDistCpProcedure() {}

  public RouterDistCpProcedure(String name, String nextProcedure,
      long delayDuration, FedBalanceContext context) throws IOException {
    super(name, nextProcedure, delayDuration, context);
  }

  /**
   * Disable write by making the mount entry readonly.
   */
  @Override
  protected void disableWrite(FedBalanceContext context) throws IOException {
    Configuration conf = context.getConf();
    String mount = context.getMount();
    MountTableProcedure.disableWrite(mount, conf);
    updateStage(Stage.FINAL_DISTCP);
  }

  /**
   * Enable write.
   */
  @Override
  protected void enableWrite() throws IOException {
    // do nothing.
  }
}
