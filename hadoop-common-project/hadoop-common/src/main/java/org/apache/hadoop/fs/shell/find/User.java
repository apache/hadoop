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
import java.util.Deque;

import org.apache.hadoop.fs.shell.PathData;

/**
 * Implements the -user expression for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
final class User extends BaseExpression {
  /**
   * Registers this expression with the specified factory.
   */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(User.class, "-user");
  }

  private static final String[] USAGE = {"-user username"};
  private static final String[] HELP =
      {"Evaluates as true if the owner of the file matches the", "specified user."};

  private String requiredUser;

  User() {
    super();
    setUsage(USAGE);
    setHelp(HELP);
  }

  @Override
  public void addArguments(Deque<String> args) {
    addArguments(args, 1);
  }

  @Override
  public void prepare() throws IOException {
    requiredUser = getArgument(1);
  }

  @Override
  public Result apply(PathData item, int depth) throws IOException {
    if (requiredUser.equals(getFileStatus(item, depth).getOwner())) {
      return Result.PASS;
    }
    return Result.FAIL;
  }
}
