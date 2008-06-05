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
package org.apache.hadoop.fs.shell;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Count the number of directories, files and bytes.
 */
public class Count {
  public static final String NAME = "count";
  public static final String USAGE = "-" + NAME + " <path>";
  public static final String DESCRIPTION = CommandUtils.formatDescription(USAGE, 
      "Count the number of directories, files and bytes under the paths",
      "that match the specified file pattern.  The output columns are:",
      "DIR_COUNT FILE_COUNT CONTENT_SIZE FILE_NAME");

  public static boolean matches(String cmd) {
    return ("-" + NAME).equals(cmd); 
  }

  public static void count(String src, Configuration conf, PrintStream out
      ) throws IOException {
    Path srcPath = new Path(src);
    FileSystem srcFs = srcPath.getFileSystem(conf);
    FileStatus[] statuses = srcFs.globStatus(srcPath);
    if (statuses == null || statuses.length == 0) {
      throw new FileNotFoundException(src + " not found.");
    }
    for(FileStatus s : statuses) {
      Path p = s.getPath();
      String pathstr = p.toString();
      out.println(srcFs.getContentSummary(p)
          + ("".equals(pathstr)? ".": pathstr));
    }
  }
}