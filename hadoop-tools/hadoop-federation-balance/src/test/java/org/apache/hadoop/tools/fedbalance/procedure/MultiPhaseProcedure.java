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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This simulates a procedure with many phases. This is used for test.
 */
public class MultiPhaseProcedure extends BalanceProcedure {

  private int totalPhase;
  private int currentPhase = 0;
  private Configuration conf;
  private FileSystem fs;
  private Path path;

  public MultiPhaseProcedure() {}

  public MultiPhaseProcedure(String name, long delay, int totalPhase,
      Configuration config, String spath) throws IOException {
    super(name, delay);
    this.totalPhase = totalPhase;
    this.conf = config;
    this.path = new Path(spath);
    this.fs = path.getFileSystem(config);
  }

  @Override
  public boolean execute() throws IOException {
    if (currentPhase < totalPhase) {
      LOG.info("Current phase {}", currentPhase);
      Path phase = new Path(path, "phase-" + currentPhase);
      if (!fs.exists(phase)) {
        fs.mkdirs(phase);
      }
      currentPhase++;
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
      }
      return false;
    }
    return true;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(totalPhase);
    out.writeInt(currentPhase);
    conf.write(out);
    Text.writeString(out, path.toString());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    totalPhase = in.readInt();
    currentPhase = in.readInt();
    conf = new Configuration(false);
    conf.readFields(in);
    path = new Path(Text.readString(in));
    fs = path.getFileSystem(conf);
  }
}
