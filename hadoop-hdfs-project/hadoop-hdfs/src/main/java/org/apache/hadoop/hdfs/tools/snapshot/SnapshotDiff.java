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
package org.apache.hadoop.hdfs.tools.snapshot;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A tool used to get the difference report between two snapshots, or between
 * a snapshot and the current status of a directory. 
 * <pre>
 * Usage: SnapshotDiff snapshotDir from to
 * For from/to, users can use "." to present the current status, and use 
 * ".snapshot/snapshot_name" to present a snapshot, where ".snapshot/" can be 
 * omitted.
 * </pre>
 */
@InterfaceAudience.Private
public class SnapshotDiff extends Configured implements Tool {
  /**
   * Construct a SnapshotDiff object.
   */
  public SnapshotDiff() {
    this(new HdfsConfiguration());
  }

  /**
   * Construct a SnapshotDiff object.
   */
  public SnapshotDiff(Configuration conf) {
    super(conf);
  }
  private static String getSnapshotName(String name) {
    if (Path.CUR_DIR.equals(name)) { // current directory
      return "";
    }
    final int i;
    if (name.startsWith(HdfsConstants.DOT_SNAPSHOT_DIR + Path.SEPARATOR)) {
      i = 0;
    } else if (name.startsWith(
        HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR + Path.SEPARATOR)) {
      i = 1;
    } else {
      return name;
    }

    // get the snapshot name
    return name.substring(i + HdfsConstants.DOT_SNAPSHOT_DIR.length() + 1);
  }
  
  @Override
  public int run(String[] argv) throws Exception {
    String description = "hdfs snapshotDiff <snapshotDir> <from> <to>:\n" +
    "\tGet the difference between two snapshots, \n" + 
    "\tor between a snapshot and the current tree of a directory.\n" +
    "\tFor <from>/<to>, users can use \".\" to present the current status,\n" +
    "\tand use \".snapshot/snapshot_name\" to present a snapshot,\n" +
    "\twhere \".snapshot/\" can be omitted\n";
    
    if(argv.length != 3) {
      System.err.println("Usage: \n" + description);
      return 1;
    }

    FileSystem fs = FileSystem.get(new Path(argv[0]).toUri(), getConf());
    if (! (fs instanceof DistributedFileSystem)) {
      System.err.println(
          "SnapshotDiff can only be used in DistributedFileSystem");
      return 1;
    }
    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    
    Path snapshotRoot = new Path(argv[0]);
    String fromSnapshot = getSnapshotName(argv[1]);
    String toSnapshot = getSnapshotName(argv[2]);
    try {
      SnapshotDiffReport diffReport = dfs.getSnapshotDiffReport(snapshotRoot,
          fromSnapshot, toSnapshot);
      System.out.println(diffReport.toString());
    } catch (IOException e) {
      String[] content = e.getLocalizedMessage().split("\n");
      System.err.println("snapshotDiff: " + content[0]);
      e.printStackTrace(System.err);
      return 1;
    }
    return 0;
  }

  public static void main(String[] argv) throws Exception {
    int rc = ToolRunner.run(new SnapshotDiff(), argv);
    System.exit(rc);
  }

}
