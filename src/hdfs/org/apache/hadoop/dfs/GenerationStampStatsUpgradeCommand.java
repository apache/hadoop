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
package org.apache.hadoop.dfs;

import java.io.*;

/**
 * The Datanode sends this statistics object to the Namenode periodically
 * during a Generation Stamp Upgrade.
 */
class GenerationStampStatsUpgradeCommand extends UpgradeCommand {
  DatanodeID datanodeId;
  int blocksUpgraded;
  int blocksRemaining;
  int errors;

  GenerationStampStatsUpgradeCommand() {
    super(GenerationStampUpgradeNamenode.DN_CMD_STATS, 0, (short)0);
    datanodeId = new DatanodeID();
  }

  public GenerationStampStatsUpgradeCommand(short status, DatanodeID dn,
                              int blocksUpgraded, int blocksRemaining,
                              int errors, int version) {
    super(GenerationStampUpgradeNamenode.DN_CMD_STATS, version, status);
    //copy so that only ID part gets serialized
    datanodeId = new DatanodeID(dn); 
    this.blocksUpgraded = blocksUpgraded;
    this.blocksRemaining = blocksRemaining;
    this.errors = errors;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    datanodeId.readFields(in);
    blocksUpgraded = in.readInt();
    blocksRemaining = in.readInt();
    errors = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    datanodeId.write(out);
    out.writeInt(blocksUpgraded);
    out.writeInt(blocksRemaining);
    out.writeInt(errors);
  }
}

