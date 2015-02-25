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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.CommandFactory;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.fs.shell.FsCommand;
import org.apache.hadoop.fs.shell.PathData;

@InterfaceAudience.Private
@InterfaceStability.Unstable
/**
 * Implements a Hadoop find command.
 */
public class Find extends FsCommand {
  /**
   * Register the names for the count command
   * 
   * @param factory the command factory that will instantiate this class
   */
  public static void registerCommands(CommandFactory factory) {
    factory.addClass(Find.class, "-find");
  }

  public static final String NAME = "find";
  public static final String USAGE = "<path> ... <expression> ...";
  public static final String DESCRIPTION;
  private static String[] HELP =
  { "Finds all files that match the specified expression and",
      "applies selected actions to them. If no <path> is specified",
      "then defaults to the current working directory. If no",
      "expression is specified then defaults to -print."
  };

  private static final String OPTION_FOLLOW_LINK = "L";
  private static final String OPTION_FOLLOW_ARG_LINK = "H";

  /** List of expressions recognized by this command. */
  private static final Set<Class<? extends Expression>> EXPRESSIONS =
      new HashSet<>();

  private static void addExpression(Class<?> clazz) {
    EXPRESSIONS.add(clazz.asSubclass(Expression.class));
  }

  static {
    // Initialize the static variables.
    // Operator Expressions
    addExpression(And.class);

    // Action Expressions
    addExpression(Print.class);

    // Navigation Expressions
    // Matcher Expressions
    addExpression(Name.class);

    DESCRIPTION = buildDescription(ExpressionFactory.getExpressionFactory());

    // Register the expressions with the expression factory.
    registerExpressions(ExpressionFactory.getExpressionFactory());
  }

  /** Options for use in this command */
  private FindOptions options;

  /** Root expression for this instance of the command. */
  private Expression rootExpression;

  /** Set of path items returning a {@link Result#STOP} result. */
  private HashSet<Path> stopPaths = new HashSet<Path>();

  /** Register the expressions with the expression factory. */
  private static void registerExpressions(ExpressionFactory factory) {
    for (Class<? extends Expression> exprClass : EXPRESSIONS) {
      factory.registerExpression(exprClass);
    }
  }

  /** Build the description used by the help command. */
  private static String buildDescription(ExpressionFactory factory) {
    ArrayList<Expression> operators = new ArrayList<Expression>();
    ArrayList<Expression> primaries = new ArrayList<Expression>();
    for (Class<? extends Expression> exprClass : EXPRESSIONS) {
      Expression expr = factory.createExpression(exprClass, null);
      if (expr.isOperator()) {
        operators.add(expr);
      } else {
        primaries.add(expr);
      }
    }
    Collections.sort(operators, new Comparator<Expression>() {
      @Override
      public int compare(Expression arg0, Expression arg1) {
        return arg0.getClass().getName().compareTo(arg1.getClass().getName());
      }
    });
    Collections.sort(primaries, new Comparator<Expression>() {
      @Override
      public int compare(Expression arg0, Expression arg1) {
        return arg0.getClass().getName().compareTo(arg1.getClass().getName());
      }
    });

    StringBuilder sb = new StringBuilder();
    for (String line : HELP) {
      sb.append(line).append("\n");
    }
    sb.append("\n");
    sb.append("The following primary expressions are recognised:\n");
    for (Expression expr : primaries) {
      for (String line : expr.getUsage()) {
        sb.append("  ").append(line).append("\n");
      }
      for (String line : expr.getHelp()) {
        sb.append("    ").append(line).append("\n");
      }
      sb.append("\n");
    }
    sb.append("The following operators are recognised:\n");
    for (Expression expr : operators) {
      for (String line : expr.getUsage()) {
        sb.append("  ").append(line).append("\n");
      }
      for (String line : expr.getHelp()) {
        sb.append("    ").append(line).append("\n");
      }
      sb.append("\n");
    }
    return sb.toString();
  }

  /** Default constructor for the Find command. */
  public Find() {
    setRecursive(true);
  }

  @Override
  protected void processOptions(LinkedList<String> args) throws IOException {
    CommandFormat cf =
        new CommandFormat(1, Integer.MAX_VALUE, OPTION_FOLLOW_LINK,
            OPTION_FOLLOW_ARG_LINK, null);
    cf.parse(args);

    if (cf.getOpt(OPTION_FOLLOW_LINK)) {
      getOptions().setFollowLink(true);
    } else if (cf.getOpt(OPTION_FOLLOW_ARG_LINK)) {
      getOptions().setFollowArgLink(true);
    }

    // search for first non-path argument (ie starts with a "-") and capture and
    // remove the remaining arguments as expressions
    LinkedList<String> expressionArgs = new LinkedList<String>();
    Iterator<String> it = args.iterator();
    boolean isPath = true;
    while (it.hasNext()) {
      String arg = it.next();
      if (isPath) {
        if (arg.startsWith("-")) {
          isPath = false;
        }
      }
      if (!isPath) {
        expressionArgs.add(arg);
        it.remove();
      }
    }

    if (args.isEmpty()) {
      args.add(Path.CUR_DIR);
    }

    Expression expression = parseExpression(expressionArgs);
    if (!expression.isAction()) {
      Expression and = getExpression(And.class);
      Deque<Expression> children = new LinkedList<Expression>();
      children.add(getExpression(Print.class));
      children.add(expression);
      and.addChildren(children);
      expression = and;
    }

    setRootExpression(expression);
  }

  /**
   * Set the root expression for this find.
   * 
   * @param expression
   */
  @InterfaceAudience.Private
  void setRootExpression(Expression expression) {
    this.rootExpression = expression;
  }

  /**
   * Return the root expression for this find.
   * 
   * @return the root expression
   */
  @InterfaceAudience.Private
  Expression getRootExpression() {
    return this.rootExpression;
  }

  /** Returns the current find options, creating them if necessary. */
  @InterfaceAudience.Private
  FindOptions getOptions() {
    if (options == null) {
      options = createOptions();
    }
    return options;
  }

  /** Create a new set of find options. */
  private FindOptions createOptions() {
    FindOptions options = new FindOptions();
    options.setOut(out);
    options.setErr(err);
    options.setIn(System.in);
    options.setCommandFactory(getCommandFactory());
    options.setConfiguration(getConf());
    return options;
  }

  /** Add the {@link PathData} item to the stop set. */
  private void addStop(PathData item) {
    stopPaths.add(item.path);
  }

  /** Returns true if the {@link PathData} item is in the stop set. */
  private boolean isStop(PathData item) {
    return stopPaths.contains(item.path);
  }

  /**
   * Parse a list of arguments to to extract the {@link Expression} elements.
   * The input Deque will be modified to remove the used elements.
   * 
   * @param args arguments to be parsed
   * @return list of {@link Expression} elements applicable to this command
   * @throws IOException if list can not be parsed
   */
  private Expression parseExpression(Deque<String> args) throws IOException {
    Deque<Expression> primaries = new LinkedList<Expression>();
    Deque<Expression> operators = new LinkedList<Expression>();
    Expression prevExpr = getExpression(And.class);
    while (!args.isEmpty()) {
      String arg = args.pop();
      if ("(".equals(arg)) {
        Expression expr = parseExpression(args);
        primaries.add(expr);
        prevExpr = new BaseExpression() {
          @Override
          public Result apply(PathData item, int depth) throws IOException {
            return Result.PASS;
          }
        }; // stub the previous expression to be a non-op
      } else if (")".equals(arg)) {
        break;
      } else if (isExpression(arg)) {
        Expression expr = getExpression(arg);
        expr.addArguments(args);
        if (expr.isOperator()) {
          while (!operators.isEmpty()) {
            if (operators.peek().getPrecedence() >= expr.getPrecedence()) {
              Expression op = operators.pop();
              op.addChildren(primaries);
              primaries.push(op);
            } else {
              break;
            }
          }
          operators.push(expr);
        } else {
          if (!prevExpr.isOperator()) {
            Expression and = getExpression(And.class);
            while (!operators.isEmpty()) {
              if (operators.peek().getPrecedence() >= and.getPrecedence()) {
                Expression op = operators.pop();
                op.addChildren(primaries);
                primaries.push(op);
              } else {
                break;
              }
            }
            operators.push(and);
          }
          primaries.push(expr);
        }
        prevExpr = expr;
      } else {
        throw new IOException("Unexpected argument: " + arg);
      }
    }

    while (!operators.isEmpty()) {
      Expression operator = operators.pop();
      operator.addChildren(primaries);
      primaries.push(operator);
    }

    return primaries.isEmpty() ? getExpression(Print.class) : primaries.pop();
  }

  /** Returns true if the target is an ancestor of the source. */
  private boolean isAncestor(PathData source, PathData target) {
    for (Path parent = source.path; (parent != null) && !parent.isRoot();
        parent = parent.getParent()) {
      if (parent.equals(target.path)) {
        return true;
      }
    }
    return false;
  }

  @Override
  protected void recursePath(PathData item) throws IOException {
    if (isStop(item)) {
      // this item returned a stop result so don't recurse any further
      return;
    }
    if (getDepth() >= getOptions().getMaxDepth()) {
      // reached the maximum depth so don't got any further.
      return;
    }
    if (item.stat.isSymlink() && getOptions().isFollowLink()) {
      PathData linkedItem =
          new PathData(item.stat.getSymlink().toString(), getConf());
      if (isAncestor(item, linkedItem)) {
        getOptions().getErr().println(
            "Infinite loop ignored: " + item.toString() + " -> "
                + linkedItem.toString());
        return;
      }
      if (linkedItem.exists) {
        item = linkedItem;
      }
    }
    if (item.stat.isDirectory()) {
      super.recursePath(item);
    }
  }

  @Override
  protected boolean isPathRecursable(PathData item) throws IOException {
    if (item.stat.isDirectory()) {
      return true;
    }
    if (item.stat.isSymlink()) {
      PathData linkedItem =
          new PathData(item.fs.resolvePath(item.stat.getSymlink()).toString(),
              getConf());
      if (linkedItem.stat.isDirectory()) {
        if (getOptions().isFollowLink()) {
          return true;
        }
        if (getOptions().isFollowArgLink() && (getDepth() == 0)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  protected void processPath(PathData item) throws IOException {
    if (getOptions().isDepthFirst()) {
      // depth first so leave until post processing
      return;
    }
    applyItem(item);
  }

  @Override
  protected void postProcessPath(PathData item) throws IOException {
    if (!getOptions().isDepthFirst()) {
      // not depth first so already processed
      return;
    }
    applyItem(item);
  }

  private void applyItem(PathData item) throws IOException {
    if (getDepth() >= getOptions().getMinDepth()) {
      Result result = getRootExpression().apply(item, getDepth());
      if (Result.STOP.equals(result)) {
        addStop(item);
      }
    }
  }

  @Override
  protected void processArguments(LinkedList<PathData> args)
      throws IOException {
    Expression expr = getRootExpression();
    expr.setOptions(getOptions());
    expr.prepare();
    super.processArguments(args);
    expr.finish();
  }

  /** Gets a named expression from the factory. */
  private Expression getExpression(String expressionName) {
    return ExpressionFactory.getExpressionFactory().getExpression(
        expressionName, getConf());
  }

  /** Gets an instance of an expression from the factory. */
  private Expression getExpression(
      Class<? extends Expression> expressionClass) {
    return ExpressionFactory.getExpressionFactory().createExpression(
        expressionClass, getConf());
  }

  /** Asks the factory whether an expression is recognized. */
  private boolean isExpression(String expressionName) {
    return ExpressionFactory.getExpressionFactory()
        .isExpression(expressionName);
  }
}
