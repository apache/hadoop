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

package org.apache.hadoop.tools.mapred;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.util.RetriableCommand;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Mapper;

import static org.apache.hadoop.tools.mapred.CopyMapper.getFileAttributeSettings;

/**
 * This class extends Retriable command to implement the creation of directories
 * with retries on failure.
 */
public class RetriableDirectoryCreateCommand extends RetriableCommand {

  /**
   * Constructor, taking a description of the action.
   * @param description Verbose description of the copy operation.
   */
  public RetriableDirectoryCreateCommand(String description) {
    super(description);
  }

  /**
   * Implementation of RetriableCommand::doExecute().
   * This implements the actual mkdirs() functionality.
   * @param arguments Argument-list to the command.
   * @return Boolean. True, if the directory could be created successfully.
   * @throws Exception IOException, on failure to create the directory.
   */
  @Override
  protected Object doExecute(Object... arguments) throws Exception {
    assert arguments.length == 3 : "Unexpected argument list.";
    Path target = (Path)arguments[0];
    Mapper.Context context = (Mapper.Context)arguments[1];
    FileStatus sourceStatus = (FileStatus)arguments[2];

    FileSystem targetFS = target.getFileSystem(context.getConfiguration());
    if(!targetFS.mkdirs(target)) {
      return false;
    }

    boolean preserveEC = getFileAttributeSettings(context)
        .contains(DistCpOptions.FileAttribute.ERASURECODINGPOLICY);
    if (preserveEC && sourceStatus.isErasureCoded()
        && targetFS instanceof DistributedFileSystem) {
      ErasureCodingPolicy ecPolicy =
          ((HdfsFileStatus) sourceStatus).getErasureCodingPolicy();
      DistributedFileSystem dfs = (DistributedFileSystem) targetFS;
      dfs.setErasureCodingPolicy(target, ecPolicy.getName());
    }
    return true;
  }
}
