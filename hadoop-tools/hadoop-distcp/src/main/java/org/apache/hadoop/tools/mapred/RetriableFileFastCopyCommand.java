/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.tools.mapred;

import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.FastCopy;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.apache.hadoop.tools.DistCpOptions.FileAttribute;
import org.apache.hadoop.tools.mapred.CopyMapper.FileAction;

public class RetriableFileFastCopyCommand extends RetriableFileCopyCommand {
  public RetriableFileFastCopyCommand(String description, FileAction action) {
    super(description, action);
  }

  public RetriableFileFastCopyCommand(boolean skipCrc, String description, FileAction action) {
    super(skipCrc, description, action);
  }

  public RetriableFileFastCopyCommand(boolean skipCrc, String description, FileAction action,
      boolean directWrite) {
    super(skipCrc, description, action, directWrite);
  }

  @Override
  protected long copyToFile(Path targetPath, FileSystem targetFS, CopyListingFileStatus source,
      long sourceOffset, Context context, EnumSet<FileAttribute> fileAttributes,
      FileChecksum sourceChecksum, FileStatus sourceStatus) throws IOException {
    FastCopy fastCopy = new FastCopy(context.getConfiguration(), source.getPath(), targetPath,
        action == FileAction.OVERWRITE || action == FileAction.APPEND, source.getChunkOffset(),
        source.getChunkLength());
    fastCopy.copyFile();

    if (action == FileAction.APPEND) {
      return source.getLen() - sourceOffset;
    }
    return source.getSizeToCopy();
  }
}
