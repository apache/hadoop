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
package org.apache.hadoop.fs.shell.find;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.shell.PathData;

/**
 * Implements the -empty expression for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
final class Empty extends BaseExpression {
  /**
   * Registers this expression with the specified factory.
   */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(Empty.class, "-empty");
  }

  private static final String[] USAGE = {"-empty"};
  private static final String[] HELP =
      {"Evaluates as true if the file is empty or directory has no", "contents."};

  Empty() {
    super();
    setUsage(USAGE);
    setHelp(HELP);
  }

  @Override
  public Result apply(PathData item, int depth) throws IOException {
    FileStatus fileStatus = getFileStatus(item, depth);
    if (fileStatus.isDirectory()) {
      if (getFileSystem(item).listStatus(getPath(item)).length == 0) {
        return Result.PASS;
      }
      return Result.FAIL;
    }
    if (fileStatus.getLen() == 0L) {
      return Result.PASS;
    }
    return Result.FAIL;
  }
}
