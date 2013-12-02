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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;

/**
 * A tool used to list all snapshottable directories that are owned by the 
 * current user. The tool returns all the snapshottable directories if the user
 * is a super user.
 */
@InterfaceAudience.Private
public class LsSnapshottableDir {
  public static void main(String[] argv) throws IOException {
    String description = "LsSnapshottableDir: \n" +
        "\tGet the list of snapshottable directories that are owned by the current user.\n" +
        "\tReturn all the snapshottable directories if the current user is a super user.\n";

    if(argv.length != 0) {
      System.err.println("Usage: \n" + description);
      System.exit(1);
    }
    
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    if (! (fs instanceof DistributedFileSystem)) {
      System.err.println(
          "LsSnapshottableDir can only be used in DistributedFileSystem");
      System.exit(1);
    }
    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    
    try {
      SnapshottableDirectoryStatus[] stats = dfs.getSnapshottableDirListing();
      SnapshottableDirectoryStatus.print(stats, System.out);
    } catch (IOException e) {
      String[] content = e.getLocalizedMessage().split("\n");
      System.err.println("lsSnapshottableDir: " + content[0]);
    }
  }

}
