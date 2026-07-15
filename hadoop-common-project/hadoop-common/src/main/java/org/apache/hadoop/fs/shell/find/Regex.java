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
import java.util.regex.Pattern;

import org.apache.hadoop.fs.shell.PathData;

/**
 * Implements the -name expression for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
final class Regex extends BaseExpression {
  /**
   * Registers this expression with the specified factory.
   */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(Regex.class, "-regex");
    factory.addClass(Iregex.class, "-iregex");
  }

  private static final String[] USAGE = {"-regex pattern", "-iregex pattern"};
  private static final String[] HELP = {"Evaluates as true if the whole file path matches the",
      "regular expression pattern. If -iregex is used then", "the match is case insensitive."};

  private final RegexMatcher matcher = new JavaRegexMatcher();
  private boolean caseSensitive = true;

  Regex() {
    this(true);
  }

  /**
   * Construct a new regex {@link Expression}.
   *
   * @param caseSensitive if true then a case-sensitive match will be performed
   */
  private Regex(boolean caseSensitive) {
    super();
    setUsage(USAGE);
    setHelp(HELP);
    setCaseSensitive(caseSensitive);
  }

  private void setCaseSensitive(boolean caseSensitive) {
    this.caseSensitive = caseSensitive;
  }

  @Override
  public void addArguments(Deque<String> args) {
    addArguments(args, 1);
  }

  @Override
  public void prepare() throws IOException {
    String pattern = getArgument(1);
    matcher.setup(pattern, caseSensitive);
  }

  @Override
  public Result apply(PathData item, int depth) throws IOException {
    String path = getPath(item).toString();
    if (matcher.matches(path)) {
      return Result.PASS;
    } else {
      return Result.FAIL;
    }
  }

  /**
   * Case-insensitive version of the -name expression.
   */
  static class Iregex extends FilterExpression {
    Iregex() {
      super(new Regex(false));
    }
  }

  private interface RegexMatcher {
    void setup(String regex, boolean caseSensitive);

    boolean matches(String input);
  }

  /**
   * Regex matcher using Java regular expression syntax.
   */
  private static final class JavaRegexMatcher implements RegexMatcher {
    private Pattern pattern;

    public void setup(String regex, boolean caseSensitive) {
      int flags = 0;
      if (!caseSensitive) {
        flags = Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE;
      }
      pattern = Pattern.compile(regex, flags);
    }

    public boolean matches(String input) {
      return pattern.matcher(input).matches();
    }
  }
}
