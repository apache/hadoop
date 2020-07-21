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
package org.apache.hadoop.tools.fedbalance;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.tools.fedbalance.procedure.BalanceProcedure;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.TrashOption;

/**
 * This procedure moves the source path to the corresponding trash.
 */
public class TrashProcedure extends BalanceProcedure {

  private DistributedFileSystem srcFs;
  private FedBalanceContext context;
  private Configuration conf;

  public TrashProcedure() {}

  /**
   * The constructor of TrashProcedure.
   *
   * @param name the name of the procedure.
   * @param nextProcedure the name of the next procedure.
   * @param delayDuration the delay duration when this procedure is delayed.
   * @param context the federation balance context.
   */
  public TrashProcedure(String name, String nextProcedure, long delayDuration,
      FedBalanceContext context) throws IOException {
    super(name, nextProcedure, delayDuration);
    this.context = context;
    this.conf = context.getConf();
    this.srcFs = (DistributedFileSystem) context.getSrc().getFileSystem(conf);
  }

  @Override
  public boolean execute() throws IOException {
    moveToTrash();
    return true;
  }

  /**
   * Delete source path to trash.
   */
  void moveToTrash() throws IOException {
    Path src = context.getSrc();
    if (srcFs.exists(src)) {
      TrashOption trashOption = context.getTrashOpt();
      switch (trashOption) {
      case TRASH:
        conf.setFloat(FS_TRASH_INTERVAL_KEY, 60);
        if (!Trash.moveToAppropriateTrash(srcFs, src, conf)) {
          throw new IOException("Failed move " + src + " to trash.");
        }
        break;
      case DELETE:
        if (!srcFs.delete(src, true)) {
          throw new IOException("Failed delete " + src);
        }
        LOG.info("{} is deleted.", src);
        break;
      case SKIP:
        break;
      default:
        throw new IOException("Unexpected trash option=" + trashOption);
      }
    }
  }

  public FedBalanceContext getContext() {
    return context;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    context.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    context = new FedBalanceContext();
    context.readFields(in);
    conf = context.getConf();
    srcFs = (DistributedFileSystem) context.getSrc().getFileSystem(conf);
  }
}
