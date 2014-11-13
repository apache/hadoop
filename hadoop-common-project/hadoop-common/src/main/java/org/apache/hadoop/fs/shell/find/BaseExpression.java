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
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.PathData;

/**
 * Abstract expression for use in the
 * {@link org.apache.hadoop.fs.shell.find.Find} command. Provides default
 * behavior for a no-argument primary expression.
 */
public abstract class BaseExpression implements Expression, Configurable {
  private String[] usage = { "Not yet implemented" };
  private String[] help = { "Not yet implemented" };

  /** Sets the usage text for this {@link Expression} */
  protected void setUsage(String[] usage) {
    this.usage = usage;
  }

  /** Sets the help text for this {@link Expression} */
  protected void setHelp(String[] help) {
    this.help = help;
  }

  @Override
  public String[] getUsage() {
    return this.usage;
  }

  @Override
  public String[] getHelp() {
    return this.help;
  }

  @Override
  public void setOptions(FindOptions options) throws IOException {
    this.options = options;
    for (Expression child : getChildren()) {
      child.setOptions(options);
    }
  }

  @Override
  public void prepare() throws IOException {
    for (Expression child : getChildren()) {
      child.prepare();
    }
  }

  @Override
  public void finish() throws IOException {
    for (Expression child : getChildren()) {
      child.finish();
    }
  }

  /** Options passed in from the {@link Find} command. */
  private FindOptions options;

  /** Hadoop configuration. */
  private Configuration conf;

  /** Arguments for this expression. */
  private LinkedList<String> arguments = new LinkedList<String>();

  /** Children of this expression. */
  private LinkedList<Expression> children = new LinkedList<Expression>();

  /** Return the options to be used by this expression. */
  protected FindOptions getOptions() {
    return (this.options == null) ? new FindOptions() : this.options;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append("(");
    boolean firstArg = true;
    for (String arg : getArguments()) {
      if (!firstArg) {
        sb.append(",");
      } else {
        firstArg = false;
      }
      sb.append(arg);
    }
    sb.append(";");
    firstArg = true;
    for (Expression child : getChildren()) {
      if (!firstArg) {
        sb.append(",");
      } else {
        firstArg = false;
      }
      sb.append(child.toString());
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  public boolean isAction() {
    for (Expression child : getChildren()) {
      if (child.isAction()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isOperator() {
    return false;
  }

  /**
   * Returns the arguments of this expression
   *
   * @return list of argument strings
   */
  protected List<String> getArguments() {
    return this.arguments;
  }

  /**
   * Returns the argument at the given position (starting from 1).
   *
   * @param position
   *          argument to be returned
   * @return requested argument
   * @throws IOException
   *           if the argument doesn't exist or is null
   */
  protected String getArgument(int position) throws IOException {
    if (position > this.arguments.size()) {
      throw new IOException("Missing argument at " + position);
    }
    String argument = this.arguments.get(position - 1);
    if (argument == null) {
      throw new IOException("Null argument at position " + position);
    }
    return argument;
  }

  /**
   * Returns the children of this expression.
   *
   * @return list of child expressions
   */
  protected List<Expression> getChildren() {
    return this.children;
  }

  @Override
  public int getPrecedence() {
    return 0;
  }

  @Override
  public void addChildren(Deque<Expression> exprs) {
    // no children by default, will be overridden by specific expressions.
  }

  /**
   * Add a specific number of children to this expression. The children are
   * popped off the head of the expressions.
   *
   * @param exprs
   *          deque of expressions from which to take the children
   * @param count
   *          number of children to be added
   */
  protected void addChildren(Deque<Expression> exprs, int count) {
    for (int i = 0; i < count; i++) {
      addChild(exprs.pop());
    }
  }

  /**
   * Add a single argument to this expression. The argument is popped off the
   * head of the expressions.
   *
   * @param expr
   *          child to add to the expression
   */
  private void addChild(Expression expr) {
    children.push(expr);
  }

  @Override
  public void addArguments(Deque<String> args) {
    // no children by default, will be overridden by specific expressions.
  }

  /**
   * Add a specific number of arguments to this expression. The children are
   * popped off the head of the expressions.
   *
   * @param args
   *          deque of arguments from which to take the argument
   * @param count
   *          number of children to be added
   */
  protected void addArguments(Deque<String> args, int count) {
    for (int i = 0; i < count; i++) {
      addArgument(args.pop());
    }
  }

  /**
   * Add a single argument to this expression. The argument is popped off the
   * head of the expressions.
   *
   * @param arg
   *          argument to add to the expression
   */
  protected void addArgument(String arg) {
    arguments.add(arg);
  }

  /**
   * Returns the {@link FileStatus} from the {@link PathData} item. If the
   * current options require links to be followed then the returned file status
   * is that of the linked file.
   *
   * @param item
   *          PathData
   * @param depth
   *          current depth in the process directories
   * @return FileStatus
   */
  protected FileStatus getFileStatus(PathData item, int depth)
      throws IOException {
    FileStatus fileStatus = item.stat;
    if (fileStatus.isSymlink()) {
      if (options.isFollowLink() || (options.isFollowArgLink() &&
          (depth == 0))) {
        Path linkedFile = item.fs.resolvePath(fileStatus.getSymlink());
        fileStatus = getFileSystem(item).getFileStatus(linkedFile);
      }
    }
    return fileStatus;
  }

  /**
   * Returns the {@link Path} from the {@link PathData} item.
   *
   * @param item
   *          PathData
   * @return Path
   */
  protected Path getPath(PathData item) throws IOException {
    return item.path;
  }

  /**
   * Returns the {@link FileSystem} associated with the {@link PathData} item.
   *
   * @param item PathData
   * @return FileSystem
   */
  protected FileSystem getFileSystem(PathData item) throws IOException {
    return item.fs;
  }
}
