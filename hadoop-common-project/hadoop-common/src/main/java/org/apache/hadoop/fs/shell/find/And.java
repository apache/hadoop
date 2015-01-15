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
 * Implements the -a (and) operator for the
 * {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
final class And extends BaseExpression {
  /** Registers this expression with the specified factory. */
  public static void registerExpression(ExpressionFactory factory)
      throws IOException {
    factory.addClass(And.class, "-a");
    factory.addClass(And.class, "-and");
  }

  private static final String[] USAGE = { "expression -a expression",
      "expression -and expression", "expression expression" };
  private static final String[] HELP = {
      "Logical AND operator for joining two expressions. Returns",
      "true if both child expressions return true. Implied by the",
      "juxtaposition of two expressions and so does not need to be",
      "explicitly specified. The second expression will not be",
      "applied if the first fails." };

  public And() {
    super();
    setUsage(USAGE);
    setHelp(HELP);
  }

  /**
   * Applies child expressions to the {@link PathData} item. If all pass then
   * returns {@link Result#PASS} else returns the result of the first
   * non-passing expression.
   */
  @Override
  public Result apply(PathData item, int depth) throws IOException {
    Result result = Result.PASS;
    for (Expression child : getChildren()) {
      Result childResult = child.apply(item, -1);
      result = result.combine(childResult);
      if (!result.isPass()) {
        return result;
      }
    }
    return result;
  }

  @Override
  public boolean isOperator() {
    return true;
  }

  @Override
  public int getPrecedence() {
    return 200;
  }

  @Override
  public void addChildren(Deque<Expression> expressions) {
    addChildren(expressions, 2);
  }
}
