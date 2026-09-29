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

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.shell.PathData;

/**
 * Implements the -perm expression for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
final class Perm extends BaseExpression {
  /**
   * Registers this expression with the specified factory.
   */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(Perm.class, "-perm");
  }

  private static final String[] USAGE = {"-perm [-]mode", "-perm [-]onum"};
  private static final String[] HELP = {"Evaluates as true if the file permissions match that",
      "specified. If the hyphen is specified then the expression",
      "shall evaluate as true if at least the bits specified",
      "match, otherwise an exact match is required.",
      "The mode may be specified using either symbolic notation,",
      "eg 'u=rwx,g+x+w' or as an octal number."};

  private int permission = 0;
  private boolean mask = false;

  Perm() {
    super();
    setUsage(USAGE);
    setHelp(HELP);
  }

  @Override
  public void prepare() throws IOException {
    parseArgument(getArgument(1));
  }

  /**
   * Parse an argument string to build the permission mask.
   *
   * @param argument String to be parsed
   * @throws IllegalArgumentException if the argument is invalid
   */
  private void parseArgument(String argument) throws IllegalArgumentException {
    String arg = argument;
    if (arg == null) {
      throw new IllegalArgumentException("Invalid null argument");
    }
    if (arg.isEmpty()) {
      throw new IllegalArgumentException("Invalid empty argument");
    }
    if (arg.startsWith("-")) {
      mask = true;
      arg = arg.substring(1);
    }
    if (Character.isDigit(arg.charAt(0))) {
      // the argument is a numeric mode
      permission = new FsPermission(arg).toShort();
    } else {
      // the argument is a symbolic mode
      for (String part : arg.split(",")) {
        int shift;
        Operator operator = null;
        int value = 0;
        int position = 0;
        switch (part.charAt(position++)) {
        case 'u':
          shift = 6;
          break;
        case 'g':
          shift = 3;
          break;
        case 'o':
          shift = 0;
          break;
        case 'a':
          shift = -1;
          break;
        default:
          throw new IllegalArgumentException("Invalid mode: " + argument);
        }
        outer:
        while (position < part.length()) {
          switch (part.charAt(position++)) {
          case '=':
            operator = EQUALS;
            break;
          case '+':
            operator = PLUS;
            break;
          case '-':
            operator = MINUS;
            break;
          default:
            throw new IllegalArgumentException("Invalid mode: " + argument);
          }
          value = 0;
          while (position < part.length()) {
            switch (part.charAt(position)) {
            case 'r':
              value |= 4;
              break;
            case 'w':
              value |= 2;
              break;
            case 'x':
              value |= 1;
              break;
            default:
              applyPermission(operator, shift, value);
              continue outer;
            }
            position++;
          }
        }
        if (operator == null || value == 0) {
          throw new IllegalArgumentException("Invalid mode: " + argument);
        }
        applyPermission(operator, shift, value);
      }
    }
  }

  private void applyPermission(Operator operator, int shift, int value) {
    if (shift != -1) {
      permission = operator.apply(permission, shift, value);
      return;
    }
    permission = operator.apply(permission, 6, value);
    permission = operator.apply(permission, 3, value);
    permission = operator.apply(permission, 0, value);
  }

  @Override
  public Result apply(PathData item, int depth) throws IOException {
    int itemPermission = getFileStatus(item, depth).getPermission().toShort();
    if (itemPermission == permission) {
      return Result.PASS;
    } else if (mask && ((itemPermission & permission) == permission)) {
      return Result.PASS;
    }
    return Result.FAIL;
  }

  @Override
  public void addArguments(Deque<String> args) {
    addArguments(args, 1);
  }

  /**
   * Interface representing an operator for defining a permission within an
   * argument.
   */
  private interface Operator {
    int apply(int current, int shift, int value);
  }

  /**
   * Operator to set a permission.
   */
  private static final Operator EQUALS = new Operator() {
    public int apply(int current, int shift, int value) {
      return (current & ~(7 << shift)) | (value << shift);
    }

    public String toString() {
      return "equals";
    }
  };

  /**
   * Operator to add a permission.
   */
  private static final Operator PLUS = new Operator() {
    public int apply(int current, int shift, int value) {
      return current | (value << shift);
    }

    public String toString() {
      return "plus";
    }
  };

  /**
   * Operator to remove a permission.
   */
  private static final Operator MINUS = new Operator() {
    public int apply(int current, int shift, int value) {
      return current & ~(value << shift);
    }

    public String toString() {
      return "minus";
    }
  };
}
