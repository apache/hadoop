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

package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hive.ql.plan.copyWork;
import org.apache.hadoop.hive.ql.parse.LoadSemanticAnalyzer;
import org.apache.hadoop.util.StringUtils;

/**
 * CopyTask implementation
 **/
public class CopyTask extends Task<copyWork> implements Serializable {

  private static final long serialVersionUID = 1L;

  public int execute() {
    FileSystem dstFs = null;
    Path toPath = null;
    try {
      Path fromPath = new Path(work.getFromPath());
      toPath = new Path(work.getToPath());

      console.printInfo("Copying data from " + fromPath.toString(), " to " + toPath.toString());

      FileSystem srcFs = fromPath.getFileSystem(conf);
      dstFs = toPath.getFileSystem(conf);

      FileStatus [] srcs = LoadSemanticAnalyzer.matchFilesOrDir(srcFs, fromPath);

      if(srcs == null || srcs.length == 0) {
        console.printError("No files matching path: " + fromPath.toString());
        return 3;
      }

      if (!dstFs.mkdirs(toPath)) {
        console.printError("Cannot make target directory: " + toPath.toString());
        return 2;
      }      

      for(FileStatus oneSrc: srcs) {
        LOG.debug("Copying file: " + oneSrc.getPath().toString());
        if(!FileUtil.copy(srcFs, oneSrc.getPath(), dstFs, toPath,
                          false, // delete source
                          true, // overwrite destination
                          conf)) {
          console.printError("Failed to copy: '"+ oneSrc.getPath().toString() +
                    "to: '" + toPath.toString() + "'");
          return 1;
        }
      }
      return 0;

    } catch (Exception e) {
      console.printError("Failed with exception " +   e.getMessage(), "\n" + StringUtils.stringifyException(e));
      return (1);
    }
  }
}
