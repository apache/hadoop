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
package org.apache.hadoop.hdfs.tools;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * A tool listing all the existing block storage policies. No argument is
 * required when using this tool.
 */
public class GetStoragePolicies extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    FileSystem fs = FileSystem.get(getConf());
    if (!(fs instanceof DistributedFileSystem)) {
      System.err.println("GetStoragePolicies can only be used against HDFS. " +
          "Please check the default FileSystem setting in your configuration.");
      return 1;
    }
    DistributedFileSystem dfs = (DistributedFileSystem) fs;

    try {
      BlockStoragePolicy[] policies = dfs.getStoragePolicies();
      System.out.println("Block Storage Policies:");
      for (BlockStoragePolicy policy : policies) {
        if (policy != null) {
          System.out.println("\t" + policy);
        }
      }
    } catch (IOException e) {
      String[] content = e.getLocalizedMessage().split("\n");
      System.err.println("GetStoragePolicies: " + content[0]);
      return 1;
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int rc = ToolRunner.run(new GetStoragePolicies(), args);
    System.exit(rc);
  }
}
