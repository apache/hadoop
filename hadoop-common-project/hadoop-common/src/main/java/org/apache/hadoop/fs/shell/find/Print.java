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

import org.apache.hadoop.fs.shell.PathData;

/**
 * Implements the -print expression for the
 * {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
final class Print extends BaseExpression {
  /** Registers this expression with the specified factory. */
  public static void registerExpression(ExpressionFactory factory)
      throws IOException {
    factory.addClass(Print.class, "-print");
    factory.addClass(Print0.class, "-print0");
  }

  private static final String[] USAGE = { "-print", "-print0" };
  private static final String[] HELP = {
      "Always evaluates to true. Causes the current pathname to be",
      "written to standard output followed by a newline. If the -print0",
      "expression is used then an ASCII NULL character is appended rather",
      "than a newline." };

  private final String suffix;

  public Print() {
    this("\n");
  }

  /**
   * Construct a Print {@link Expression} with the specified suffix.
   */
  private Print(String suffix) {
    super();
    setUsage(USAGE);
    setHelp(HELP);
    this.suffix = suffix;
  }

  @Override
  public Result apply(PathData item, int depth) throws IOException {
    getOptions().getOut().print(item.toString() + suffix);
    return Result.PASS;
  }

  @Override
  public boolean isAction() {
    return true;
  }

  /** Implements the -print0 expression. */
  final static class Print0 extends FilterExpression {
    public Print0() {
      super(new Print("\0"));
    }
  }
}
