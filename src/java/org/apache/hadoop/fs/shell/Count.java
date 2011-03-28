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

import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ContentSummary;

/**
 * Count the number of directories, files, bytes, quota, and remaining quota.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving

public class Count extends FsCommand {
  public static final String NAME = "count";
  public static final String USAGE = "-" + NAME + " [-q] <path> ...";
  public static final String DESCRIPTION = CommandUtils.formatDescription(USAGE, 
      "Count the number of directories, files and bytes under the paths",
      "that match the specified file pattern.  The output columns are:",
      "DIR_COUNT FILE_COUNT CONTENT_SIZE FILE_NAME or",
      "QUOTA REMAINING_QUATA SPACE_QUOTA REMAINING_SPACE_QUOTA ",
      "      DIR_COUNT FILE_COUNT CONTENT_SIZE FILE_NAME");
  
  private boolean showQuotas;

  @Override
  protected void processOptions(LinkedList<String> args) {
    CommandFormat cf = new CommandFormat(NAME, 1, Integer.MAX_VALUE, "q");
    cf.parse(args);
    if (args.size() == 0) { // default path is the current working directory
      args.add(".");
    }
    showQuotas = cf.getOpt("q");
  }

  @Override
  protected void processPath(PathData src) throws IOException {
    ContentSummary summary = src.fs.getContentSummary(src.path);
    out.println(summary.toString(showQuotas) + src.path);
  }
}
