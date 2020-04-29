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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.NoSuchElementException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.fs.shell.find.BaseExpression;
import org.apache.hadoop.fs.shell.find.Expression;
import org.apache.hadoop.fs.shell.find.Find;
import org.apache.hadoop.fs.shell.find.FindOptions;
import org.apache.hadoop.fs.shell.find.Result;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestFind {

  @Rule
  public Timeout timeout = new Timeout(10000);

  private static FileSystem mockFs;
  private static Configuration conf;

  @Before
  public void setup() throws IOException {
    mockFs = MockFileSystem.setup();
    conf = mockFs.getConf();
  }
  
  // check follow link option is recognized
  @Test
  public void processOptionsFollowLink() throws IOException {
    Find find = new Find();
    String args = "-L path";
    find.processOptions(getArgs(args));
    assertTrue(find.getOptions().isFollowLink());
    assertFalse(find.getOptions().isFollowArgLink());
  }

  // check follow arg link option is recognized
  @Test
  public void processOptionsFollowArgLink() throws IOException {
    Find find = new Find();
    String args = "-H path";
    find.processOptions(getArgs(args));
    assertFalse(find.getOptions().isFollowLink());
    assertTrue(find.getOptions().isFollowArgLink());
  }

  // check follow arg link option is recognized
  @Test
  public void processOptionsFollowLinkFollowArgLink() throws IOException {
    Find find = new Find();
    String args = "-L -H path";
    find.processOptions(getArgs(args));
    assertTrue(find.getOptions().isFollowLink());
    
    // follow link option takes precedence over follow arg link
    assertFalse(find.getOptions().isFollowArgLink());
  }
  
  // check options and expressions are stripped from args leaving paths
  @Test
  public void processOptionsExpression() throws IOException {
    Find find = new Find();
    find.setConf(conf);

    String paths = "path1 path2 path3";
    String args = "-L -H " + paths + " -print -name test";
    LinkedList<String> argsList = getArgs(args);
    find.processOptions(argsList);
    LinkedList<String> pathList = getArgs(paths);
    assertEquals(pathList, argsList);
  }

  // check print is used as the default expression
  @Test
  public void processOptionsNoExpression() throws IOException {
    Find find = new Find();
    find.setConf(conf);
    String args = "path";
    String expected = "Print(;)";
    find.processOptions(getArgs(args));
    Expression expression = find.getRootExpression();
    assertEquals(expected, expression.toString());
  }

  // check unknown options are rejected
  @Test
  public void processOptionsUnknown() throws IOException {
    Find find = new Find();
    find.setConf(conf);
    String args = "path -unknown";
    try {
      find.processOptions(getArgs(args));
      fail("Unknown expression not caught");
    } catch (IOException e) {
    }
  }

  // check unknown options are rejected when mixed with known options
  @Test
  public void processOptionsKnownUnknown() throws IOException {
    Find find = new Find();
    find.setConf(conf);
    String args = "path -print -unknown -print";
    try {
      find.processOptions(getArgs(args));
      fail("Unknown expression not caught");
    } catch (IOException e) {
    }
  }

  // check no path defaults to current working directory
  @Test
  public void processOptionsNoPath() throws IOException {
    Find find = new Find();
    find.setConf(conf);
    String args = "-print";
    
    LinkedList<String> argsList = getArgs(args);
    find.processOptions(argsList);
    assertEquals(Collections.singletonList(Path.CUR_DIR), argsList);
  }

  // check -name is handled correctly
  @Test
  public void processOptionsName() throws IOException {
    Find find = new Find();
    find.setConf(conf);
    String args = "path -name namemask";
    String expected = "And(;Name(namemask;),Print(;))";
    find.processOptions(getArgs(args));
    Expression expression = find.getRootExpression();
    assertEquals(expected, expression.toString());
  }

  // check -iname is handled correctly
  @Test
  public void processOptionsIname() throws IOException {
    Find find = new Find();
    find.setConf(conf);
    String args = "path -iname namemask";
    String expected = "And(;Iname-Name(namemask;),Print(;))";
    find.processOptions(getArgs(args));
    Expression expression = find.getRootExpression();
    assertEquals(expected, expression.toString());
  }

  // check -print is handled correctly
  @Test
  public void processOptionsPrint() throws IOException {
    Find find = new Find();
    find.setConf(conf);
    String args = "path -print";
    String expected = "Print(;)";
    find.processOptions(getArgs(args));
    Expression expression = find.getRootExpression();
    assertEquals(expected, expression.toString());
  }

  // check -print0 is handled correctly
  @Test
  public void processOptionsPrint0() throws IOException {
    Find find = new Find();
    find.setConf(conf);
    String args = "path -print0";
    String expected = "Print0-Print(;)";
    find.processOptions(getArgs(args));
    Expression expression = find.getRootExpression();
    assertEquals(expected, expression.toString());
  }

  // check an implicit and is handled correctly
  @Test
  public void processOptionsNoop() throws IOException {
    Find find = new Find();
    find.setConf(conf);

    String args = "path -name one -name two -print";
    String expected = "And(;And(;Name(one;),Name(two;)),Print(;))";
    find.processOptions(getArgs(args));
    Expression expression = find.getRootExpression();
    assertEquals(expected, expression.toString());
  }

  // check -a is handled correctly
  @Test
  public void processOptionsA() throws IOException {
    Find find = new Find();
    find.setConf(conf);

    String args = "path -name one -a -name two -a -print";
    String expected = "And(;And(;Name(one;),Name(two;)),Print(;))";
    find.processOptions(getArgs(args));
    Expression expression = find.getRootExpression();
    assertEquals(expected, expression.toString());
  }

  // check -and is handled correctly
  @Test
  public void processOptionsAnd() throws IOException {
    Find find = new Find();
    find.setConf(conf);

    String args = "path -name one -and -name two -and -print";
    String expected = "And(;And(;Name(one;),Name(two;)),Print(;))";
    find.processOptions(getArgs(args));
    Expression expression = find.getRootExpression();
    assertEquals(expected, expression.toString());
  }

  // check expressions are called in the correct order
  @Test
  public void processArguments() throws IOException {
    LinkedList<PathData> items = createDirectories();

    Find find = new Find();
    find.setConf(conf);
    PrintStream out = mock(PrintStream.class);
    find.getOptions().setOut(out);
    PrintStream err = mock(PrintStream.class);
    find.getOptions().setErr(err);
    Expression expr = mock(Expression.class);
    when(expr.apply((PathData) any(), anyInt())).thenReturn(Result.PASS);
    FileStatusChecker fsCheck = mock(FileStatusChecker.class);
    Expression test = new TestExpression(expr, fsCheck);
    find.setRootExpression(test);
    find.processArguments(items);

    InOrder inOrder = inOrder(expr);
    inOrder.verify(expr).setOptions(find.getOptions());
    inOrder.verify(expr).prepare();
    inOrder.verify(expr).apply(item1, 0);
    inOrder.verify(expr).apply(item1a, 1);
    inOrder.verify(expr).apply(item1aa, 2);
    inOrder.verify(expr).apply(item1b, 1);
    inOrder.verify(expr).apply(item2, 0);
    inOrder.verify(expr).apply(item3, 0);
    inOrder.verify(expr).apply(item4, 0);
    inOrder.verify(expr).apply(item5, 0);
    inOrder.verify(expr).apply(item5a, 1);
    inOrder.verify(expr).apply(item5b, 1);
    inOrder.verify(expr).apply(item5c, 1);
    inOrder.verify(expr).apply(item5ca, 2);
    inOrder.verify(expr).apply(item5d, 1);
    inOrder.verify(expr).apply(item5e, 1);
    inOrder.verify(expr).finish();
    verifyNoMoreInteractions(expr);

    InOrder inOrderFsCheck = inOrder(fsCheck);
    inOrderFsCheck.verify(fsCheck).check(item1.stat);
    inOrderFsCheck.verify(fsCheck).check(item1a.stat);
    inOrderFsCheck.verify(fsCheck).check(item1aa.stat);
    inOrderFsCheck.verify(fsCheck).check(item1b.stat);
    inOrderFsCheck.verify(fsCheck).check(item2.stat);
    inOrderFsCheck.verify(fsCheck).check(item3.stat);
    inOrderFsCheck.verify(fsCheck).check(item4.stat);
    inOrderFsCheck.verify(fsCheck).check(item5.stat);
    inOrderFsCheck.verify(fsCheck).check(item5a.stat);
    inOrderFsCheck.verify(fsCheck).check(item5b.stat);
    inOrderFsCheck.verify(fsCheck).check(item5c.stat);
    inOrderFsCheck.verify(fsCheck).check(item5ca.stat);
    inOrderFsCheck.verify(fsCheck).check(item5d.stat);
    inOrderFsCheck.verify(fsCheck).check(item5e.stat);
    verifyNoMoreInteractions(fsCheck);

    verifyNoMoreInteractions(out);
    verifyNoMoreInteractions(err);
  }

  // check that directories are descended correctly when -depth is specified
  @Test
  public void processArgumentsDepthFirst() throws IOException {
    LinkedList<PathData> items = createDirectories();

    Find find = new Find();
    find.getOptions().setDepthFirst(true);
    find.setConf(conf);
    PrintStream out = mock(PrintStream.class);
    find.getOptions().setOut(out);
    PrintStream err = mock(PrintStream.class);
    find.getOptions().setErr(err);
    Expression expr = mock(Expression.class);
    when(expr.apply((PathData) any(), anyInt())).thenReturn(Result.PASS);
    FileStatusChecker fsCheck = mock(FileStatusChecker.class);
    Expression test = new TestExpression(expr, fsCheck);
    find.setRootExpression(test);
    find.processArguments(items);

    InOrder inOrder = inOrder(expr);
    inOrder.verify(expr).setOptions(find.getOptions());
    inOrder.verify(expr).prepare();
    inOrder.verify(expr).apply(item1aa, 2);
    inOrder.verify(expr).apply(item1a, 1);
    inOrder.verify(expr).apply(item1b, 1);
    inOrder.verify(expr).apply(item1, 0);
    inOrder.verify(expr).apply(item2, 0);
    inOrder.verify(expr).apply(item3, 0);
    inOrder.verify(expr).apply(item4, 0);
    inOrder.verify(expr).apply(item5a, 1);
    inOrder.verify(expr).apply(item5b, 1);
    inOrder.verify(expr).apply(item5ca, 2);
    inOrder.verify(expr).apply(item5c, 1);
    inOrder.verify(expr).apply(item5d, 1);
    inOrder.verify(expr).apply(item5e, 1);
    inOrder.verify(expr).apply(item5, 0);
    inOrder.verify(expr).finish();
    verifyNoMoreInteractions(expr);

    InOrder inOrderFsCheck = inOrder(fsCheck);
    inOrderFsCheck.verify(fsCheck).check(item1aa.stat);
    inOrderFsCheck.verify(fsCheck).check(item1a.stat);
    inOrderFsCheck.verify(fsCheck).check(item1b.stat);
    inOrderFsCheck.verify(fsCheck).check(item1.stat);
    inOrderFsCheck.verify(fsCheck).check(item2.stat);
    inOrderFsCheck.verify(fsCheck).check(item3.stat);
    inOrderFsCheck.verify(fsCheck).check(item4.stat);
    inOrderFsCheck.verify(fsCheck).check(item5a.stat);
    inOrderFsCheck.verify(fsCheck).check(item5b.stat);
    inOrderFsCheck.verify(fsCheck).check(item5ca.stat);
    inOrderFsCheck.verify(fsCheck).check(item5c.stat);
    inOrderFsCheck.verify(fsCheck).check(item5d.stat);
    inOrderFsCheck.verify(fsCheck).check(item5e.stat);
    inOrderFsCheck.verify(fsCheck).check(item5.stat);
    verifyNoMoreInteractions(fsCheck);

    verifyNoMoreInteractions(out);
    verifyNoMoreInteractions(err);
  }

  // check symlinks given as path arguments are processed correctly with the
  // follow arg option set
  @Test
  public void processArgumentsOptionFollowArg() throws IOException {
    LinkedList<PathData> items = createDirectories();

    Find find = new Find();
    find.getOptions().setFollowArgLink(true);
    find.setConf(conf);
    PrintStream out = mock(PrintStream.class);
    find.getOptions().setOut(out);
    PrintStream err = mock(PrintStream.class);
    find.getOptions().setErr(err);
    Expression expr = mock(Expression.class);
    when(expr.apply((PathData) any(), anyInt())).thenReturn(Result.PASS);
    FileStatusChecker fsCheck = mock(FileStatusChecker.class);
    Expression test = new TestExpression(expr, fsCheck);
    find.setRootExpression(test);
    find.processArguments(items);

    InOrder inOrder = inOrder(expr);
    inOrder.verify(expr).setOptions(find.getOptions());
    inOrder.verify(expr).prepare();
    inOrder.verify(expr).apply(item1, 0);
    inOrder.verify(expr).apply(item1a, 1);
    inOrder.verify(expr).apply(item1aa, 2);
    inOrder.verify(expr).apply(item1b, 1);
    inOrder.verify(expr).apply(item2, 0);
    inOrder.verify(expr).apply(item3, 0);
    inOrder.verify(expr).apply(item4, 0);
    inOrder.verify(expr).apply(item5, 0);
    inOrder.verify(expr).apply(item5a, 1);
    inOrder.verify(expr).apply(item5b, 1);
    inOrder.verify(expr).apply(item5c, 1);
    inOrder.verify(expr).apply(item5ca, 2);
    inOrder.verify(expr).apply(item5d, 1);
    inOrder.verify(expr).apply(item5e, 1);
    inOrder.verify(expr).finish();
    verifyNoMoreInteractions(expr);

    InOrder inOrderFsCheck = inOrder(fsCheck);
    inOrderFsCheck.verify(fsCheck).check(item1.stat);
    inOrderFsCheck.verify(fsCheck).check(item1a.stat);
    inOrderFsCheck.verify(fsCheck).check(item1aa.stat);
    inOrderFsCheck.verify(fsCheck).check(item1b.stat);
    inOrderFsCheck.verify(fsCheck).check(item2.stat);
    inOrderFsCheck.verify(fsCheck, times(2)).check(item3.stat);
    inOrderFsCheck.verify(fsCheck).check(item5.stat);
    inOrderFsCheck.verify(fsCheck).check(item5a.stat);
    inOrderFsCheck.verify(fsCheck).check(item5b.stat);
    inOrderFsCheck.verify(fsCheck).check(item5c.stat);
    inOrderFsCheck.verify(fsCheck).check(item5ca.stat);
    inOrderFsCheck.verify(fsCheck).check(item5d.stat);
    inOrderFsCheck.verify(fsCheck).check(item5e.stat);
    verifyNoMoreInteractions(fsCheck);

    verifyNoMoreInteractions(out);
    verifyNoMoreInteractions(err);
  }

  // check symlinks given as path arguments are processed correctly with the
  // follow option
  @Test
  public void processArgumentsOptionFollow() throws IOException {
    LinkedList<PathData> items = createDirectories();

    Find find = new Find();
    find.getOptions().setFollowLink(true);
    find.setConf(conf);
    PrintStream out = mock(PrintStream.class);
    find.getOptions().setOut(out);
    PrintStream err = mock(PrintStream.class);
    find.getOptions().setErr(err);
    Expression expr = mock(Expression.class);
    when(expr.apply((PathData) any(), anyInt())).thenReturn(Result.PASS);
    FileStatusChecker fsCheck = mock(FileStatusChecker.class);
    Expression test = new TestExpression(expr, fsCheck);
    find.setRootExpression(test);
    find.processArguments(items);

    InOrder inOrder = inOrder(expr);
    inOrder.verify(expr).setOptions(find.getOptions());
    inOrder.verify(expr).prepare();
    inOrder.verify(expr).apply(item1, 0);
    inOrder.verify(expr).apply(item1a, 1);
    inOrder.verify(expr).apply(item1aa, 2);
    inOrder.verify(expr).apply(item1b, 1);
    inOrder.verify(expr).apply(item2, 0);
    inOrder.verify(expr).apply(item3, 0);
    inOrder.verify(expr).apply(item4, 0);
    inOrder.verify(expr).apply(item5, 0);
    inOrder.verify(expr).apply(item5a, 1);
    inOrder.verify(expr).apply(item5b, 1); // triggers infinite loop message
    inOrder.verify(expr).apply(item5c, 1);
    inOrder.verify(expr).apply(item5ca, 2);
    inOrder.verify(expr).apply(item5d, 1);
    inOrder.verify(expr).apply(item5ca, 2); // following item5d symlink
    inOrder.verify(expr).apply(item5e, 1);
    inOrder.verify(expr).finish();
    verifyNoMoreInteractions(expr);

    InOrder inOrderFsCheck = inOrder(fsCheck);
    inOrderFsCheck.verify(fsCheck).check(item1.stat);
    inOrderFsCheck.verify(fsCheck).check(item1a.stat);
    inOrderFsCheck.verify(fsCheck).check(item1aa.stat);
    inOrderFsCheck.verify(fsCheck).check(item1b.stat);
    inOrderFsCheck.verify(fsCheck).check(item2.stat);
    inOrderFsCheck.verify(fsCheck, times(2)).check(item3.stat);
    inOrderFsCheck.verify(fsCheck).check(item5.stat);
    inOrderFsCheck.verify(fsCheck).check(item1b.stat);
    inOrderFsCheck.verify(fsCheck).check(item5.stat);
    inOrderFsCheck.verify(fsCheck).check(item5c.stat);
    inOrderFsCheck.verify(fsCheck).check(item5ca.stat);
    inOrderFsCheck.verify(fsCheck).check(item5c.stat);
    inOrderFsCheck.verify(fsCheck, times(2)).check(item5ca.stat);
    verifyNoMoreInteractions(fsCheck);

    verifyNoMoreInteractions(out);
    verify(err).println(
        "Infinite loop ignored: " + item5b.toString() + " -> "
            + item5.toString());
    verifyNoMoreInteractions(err);
  }

  // check minimum depth is handledfollowLink
  @Test
  public void processArgumentsMinDepth() throws IOException {
    LinkedList<PathData> items = createDirectories();

    Find find = new Find();
    find.getOptions().setMinDepth(1);
    find.setConf(conf);
    PrintStream out = mock(PrintStream.class);
    find.getOptions().setOut(out);
    PrintStream err = mock(PrintStream.class);
    find.getOptions().setErr(err);
    Expression expr = mock(Expression.class);
    when(expr.apply((PathData) any(), anyInt())).thenReturn(Result.PASS);
    FileStatusChecker fsCheck = mock(FileStatusChecker.class);
    Expression test = new TestExpression(expr, fsCheck);
    find.setRootExpression(test);
    find.processArguments(items);

    InOrder inOrder = inOrder(expr);
    inOrder.verify(expr).setOptions(find.getOptions());
    inOrder.verify(expr).prepare();
    inOrder.verify(expr).apply(item1a, 1);
    inOrder.verify(expr).apply(item1aa, 2);
    inOrder.verify(expr).apply(item1b, 1);
    inOrder.verify(expr).apply(item5a, 1);
    inOrder.verify(expr).apply(item5b, 1);
    inOrder.verify(expr).apply(item5c, 1);
    inOrder.verify(expr).apply(item5ca, 2);
    inOrder.verify(expr).apply(item5d, 1);
    inOrder.verify(expr).apply(item5e, 1);
    inOrder.verify(expr).finish();
    verifyNoMoreInteractions(expr);

    InOrder inOrderFsCheck = inOrder(fsCheck);
    inOrderFsCheck.verify(fsCheck).check(item1a.stat);
    inOrderFsCheck.verify(fsCheck).check(item1aa.stat);
    inOrderFsCheck.verify(fsCheck).check(item1b.stat);
    inOrderFsCheck.verify(fsCheck).check(item5a.stat);
    inOrderFsCheck.verify(fsCheck).check(item5b.stat);
    inOrderFsCheck.verify(fsCheck).check(item5c.stat);
    inOrderFsCheck.verify(fsCheck).check(item5ca.stat);
    inOrderFsCheck.verify(fsCheck).check(item5d.stat);
    inOrderFsCheck.verify(fsCheck).check(item5e.stat);
    verifyNoMoreInteractions(fsCheck);

    verifyNoMoreInteractions(out);
    verifyNoMoreInteractions(err);
  }

  // check maximum depth is handled
  @Test
  public void processArgumentsMaxDepth() throws IOException {
    LinkedList<PathData> items = createDirectories();

    Find find = new Find();
    find.getOptions().setMaxDepth(1);
    find.setConf(conf);
    PrintStream out = mock(PrintStream.class);
    find.getOptions().setOut(out);
    PrintStream err = mock(PrintStream.class);
    find.getOptions().setErr(err);
    Expression expr = mock(Expression.class);
    when(expr.apply((PathData) any(), anyInt())).thenReturn(Result.PASS);
    FileStatusChecker fsCheck = mock(FileStatusChecker.class);
    Expression test = new TestExpression(expr, fsCheck);
    find.setRootExpression(test);
    find.processArguments(items);

    InOrder inOrder = inOrder(expr);
    inOrder.verify(expr).setOptions(find.getOptions());
    inOrder.verify(expr).prepare();
    inOrder.verify(expr).apply(item1, 0);
    inOrder.verify(expr).apply(item1a, 1);
    inOrder.verify(expr).apply(item1b, 1);
    inOrder.verify(expr).apply(item2, 0);
    inOrder.verify(expr).apply(item3, 0);
    inOrder.verify(expr).apply(item4, 0);
    inOrder.verify(expr).apply(item5, 0);
    inOrder.verify(expr).apply(item5a, 1);
    inOrder.verify(expr).apply(item5b, 1);
    inOrder.verify(expr).apply(item5c, 1);
    inOrder.verify(expr).apply(item5d, 1);
    inOrder.verify(expr).apply(item5e, 1);
    inOrder.verify(expr).finish();
    verifyNoMoreInteractions(expr);

    InOrder inOrderFsCheck = inOrder(fsCheck);
    inOrderFsCheck.verify(fsCheck).check(item1.stat);
    inOrderFsCheck.verify(fsCheck).check(item1a.stat);
    inOrderFsCheck.verify(fsCheck).check(item1b.stat);
    inOrderFsCheck.verify(fsCheck).check(item2.stat);
    inOrderFsCheck.verify(fsCheck).check(item3.stat);
    inOrderFsCheck.verify(fsCheck).check(item4.stat);
    inOrderFsCheck.verify(fsCheck).check(item5.stat);
    inOrderFsCheck.verify(fsCheck).check(item5a.stat);
    inOrderFsCheck.verify(fsCheck).check(item5b.stat);
    inOrderFsCheck.verify(fsCheck).check(item5c.stat);
    inOrderFsCheck.verify(fsCheck).check(item5d.stat);
    inOrderFsCheck.verify(fsCheck).check(item5e.stat);
    verifyNoMoreInteractions(fsCheck);

    verifyNoMoreInteractions(out);
    verifyNoMoreInteractions(err);
  }

  // check min depth is handled when -depth is specified
  @Test
  public void processArgumentsDepthFirstMinDepth() throws IOException {
    LinkedList<PathData> items = createDirectories();

    Find find = new Find();
    find.getOptions().setDepthFirst(true);
    find.getOptions().setMinDepth(1);
    find.setConf(conf);
    PrintStream out = mock(PrintStream.class);
    find.getOptions().setOut(out);
    PrintStream err = mock(PrintStream.class);
    find.getOptions().setErr(err);
    Expression expr = mock(Expression.class);
    when(expr.apply((PathData) any(), anyInt())).thenReturn(Result.PASS);
    FileStatusChecker fsCheck = mock(FileStatusChecker.class);
    Expression test = new TestExpression(expr, fsCheck);
    find.setRootExpression(test);
    find.processArguments(items);

    InOrder inOrder = inOrder(expr);
    inOrder.verify(expr).setOptions(find.getOptions());
    inOrder.verify(expr).prepare();
    inOrder.verify(expr).apply(item1aa, 2);
    inOrder.verify(expr).apply(item1a, 1);
    inOrder.verify(expr).apply(item1b, 1);
    inOrder.verify(expr).apply(item5a, 1);
    inOrder.verify(expr).apply(item5b, 1);
    inOrder.verify(expr).apply(item5ca, 2);
    inOrder.verify(expr).apply(item5c, 1);
    inOrder.verify(expr).apply(item5d, 1);
    inOrder.verify(expr).apply(item5e, 1);
    inOrder.verify(expr).finish();
    verifyNoMoreInteractions(expr);

    InOrder inOrderFsCheck = inOrder(fsCheck);
    inOrderFsCheck.verify(fsCheck).check(item1aa.stat);
    inOrderFsCheck.verify(fsCheck).check(item1a.stat);
    inOrderFsCheck.verify(fsCheck).check(item1b.stat);
    inOrderFsCheck.verify(fsCheck).check(item5a.stat);
    inOrderFsCheck.verify(fsCheck).check(item5b.stat);
    inOrderFsCheck.verify(fsCheck).check(item5ca.stat);
    inOrderFsCheck.verify(fsCheck).check(item5c.stat);
    inOrderFsCheck.verify(fsCheck).check(item5d.stat);
    inOrderFsCheck.verify(fsCheck).check(item5e.stat);
    verifyNoMoreInteractions(fsCheck);

    verifyNoMoreInteractions(out);
    verifyNoMoreInteractions(err);
  }

  // check max depth is handled when -depth is specified
  @Test
  public void processArgumentsDepthFirstMaxDepth() throws IOException {
    LinkedList<PathData> items = createDirectories();

    Find find = new Find();
    find.getOptions().setDepthFirst(true);
    find.getOptions().setMaxDepth(1);
    find.setConf(conf);
    PrintStream out = mock(PrintStream.class);
    find.getOptions().setOut(out);
    PrintStream err = mock(PrintStream.class);
    find.getOptions().setErr(err);
    Expression expr = mock(Expression.class);
    when(expr.apply((PathData) any(), anyInt())).thenReturn(Result.PASS);
    FileStatusChecker fsCheck = mock(FileStatusChecker.class);
    Expression test = new TestExpression(expr, fsCheck);
    find.setRootExpression(test);
    find.processArguments(items);

    InOrder inOrder = inOrder(expr);
    inOrder.verify(expr).setOptions(find.getOptions());
    inOrder.verify(expr).prepare();
    inOrder.verify(expr).apply(item1a, 1);
    inOrder.verify(expr).apply(item1b, 1);
    inOrder.verify(expr).apply(item1, 0);
    inOrder.verify(expr).apply(item2, 0);
    inOrder.verify(expr).apply(item3, 0);
    inOrder.verify(expr).apply(item4, 0);
    inOrder.verify(expr).apply(item5a, 1);
    inOrder.verify(expr).apply(item5b, 1);
    inOrder.verify(expr).apply(item5c, 1);
    inOrder.verify(expr).apply(item5d, 1);
    inOrder.verify(expr).apply(item5e, 1);
    inOrder.verify(expr).apply(item5, 0);
    inOrder.verify(expr).finish();
    verifyNoMoreInteractions(expr);

    InOrder inOrderFsCheck = inOrder(fsCheck);
    inOrderFsCheck.verify(fsCheck).check(item1a.stat);
    inOrderFsCheck.verify(fsCheck).check(item1b.stat);
    inOrderFsCheck.verify(fsCheck).check(item1.stat);
    inOrderFsCheck.verify(fsCheck).check(item2.stat);
    inOrderFsCheck.verify(fsCheck).check(item3.stat);
    inOrderFsCheck.verify(fsCheck).check(item4.stat);
    inOrderFsCheck.verify(fsCheck).check(item5a.stat);
    inOrderFsCheck.verify(fsCheck).check(item5b.stat);
    inOrderFsCheck.verify(fsCheck).check(item5c.stat);
    inOrderFsCheck.verify(fsCheck).check(item5d.stat);
    inOrderFsCheck.verify(fsCheck).check(item5e.stat);
    inOrderFsCheck.verify(fsCheck).check(item5.stat);
    verifyNoMoreInteractions(fsCheck);

    verifyNoMoreInteractions(out);
    verifyNoMoreInteractions(err);
  }

  // check expressions are called in the correct order
  @Test
  public void processArgumentsNoDescend() throws IOException {
    LinkedList<PathData> items = createDirectories();

    Find find = new Find();
    find.setConf(conf);
    PrintStream out = mock(PrintStream.class);
    find.getOptions().setOut(out);
    PrintStream err = mock(PrintStream.class);
    find.getOptions().setErr(err);
    Expression expr = mock(Expression.class);
    when(expr.apply((PathData) any(), anyInt())).thenReturn(Result.PASS);
    when(expr.apply(eq(item1a), anyInt())).thenReturn(Result.STOP);
    FileStatusChecker fsCheck = mock(FileStatusChecker.class);
    Expression test = new TestExpression(expr, fsCheck);
    find.setRootExpression(test);
    find.processArguments(items);

    InOrder inOrder = inOrder(expr);
    inOrder.verify(expr).setOptions(find.getOptions());
    inOrder.verify(expr).prepare();
    inOrder.verify(expr).apply(item1, 0);
    inOrder.verify(expr).apply(item1a, 1);
    inOrder.verify(expr).apply(item1b, 1);
    inOrder.verify(expr).apply(item2, 0);
    inOrder.verify(expr).apply(item3, 0);
    inOrder.verify(expr).apply(item4, 0);
    inOrder.verify(expr).apply(item5, 0);
    inOrder.verify(expr).apply(item5a, 1);
    inOrder.verify(expr).apply(item5b, 1);
    inOrder.verify(expr).apply(item5c, 1);
    inOrder.verify(expr).apply(item5ca, 2);
    inOrder.verify(expr).apply(item5d, 1);
    inOrder.verify(expr).apply(item5e, 1);
    inOrder.verify(expr).finish();
    verifyNoMoreInteractions(expr);

    InOrder inOrderFsCheck = inOrder(fsCheck);
    inOrderFsCheck.verify(fsCheck).check(item1.stat);
    inOrderFsCheck.verify(fsCheck).check(item1a.stat);
    inOrderFsCheck.verify(fsCheck).check(item1b.stat);
    inOrderFsCheck.verify(fsCheck).check(item2.stat);
    inOrderFsCheck.verify(fsCheck).check(item3.stat);
    inOrderFsCheck.verify(fsCheck).check(item4.stat);
    inOrderFsCheck.verify(fsCheck).check(item5.stat);
    inOrderFsCheck.verify(fsCheck).check(item5a.stat);
    inOrderFsCheck.verify(fsCheck).check(item5b.stat);
    inOrderFsCheck.verify(fsCheck).check(item5c.stat);
    inOrderFsCheck.verify(fsCheck).check(item5ca.stat);
    inOrderFsCheck.verify(fsCheck).check(item5d.stat);
    inOrderFsCheck.verify(fsCheck).check(item5e.stat);
    verifyNoMoreInteractions(fsCheck);

    verifyNoMoreInteractions(out);
    verifyNoMoreInteractions(err);
  }

  private interface FileStatusChecker {
    public void check(FileStatus fileStatus);
  }

  private class TestExpression extends BaseExpression implements Expression {
    private Expression expr;
    private FileStatusChecker checker;
    public TestExpression(Expression expr, FileStatusChecker checker) {
      this.expr = expr;
      this.checker = checker;
    }
    @Override
    public Result apply(PathData item, int depth) throws IOException {
      FileStatus fileStatus = getFileStatus(item, depth);
      checker.check(fileStatus);
      return expr.apply(item, depth);
    }
    @Override
    public void setOptions(FindOptions options) throws IOException {
      super.setOptions(options);
      expr.setOptions(options);
    }
    @Override
    public void prepare() throws IOException {
      expr.prepare();
    }
    @Override
    public void finish() throws IOException {
      expr.finish();
    }
  }

  // creates a directory structure for traversal
  // item1 (directory)
  // \- item1a (directory)
  //    \- item1aa (file)
  // \- item1b (file)
  // item2 (directory)
  // item3 (file)
  // item4 (link) -> item3
  // item5 (directory)
  // \- item5a (link) -> item1b
  // \- item5b (link) -> item5 (infinite loop)
  // \- item5c (directory)
  //    \- item5ca (file)
  // \- item5d (link) -> item5c
  // \- item5e (link) -> item5c/item5ca
  private PathData item1 = null;
  private PathData item1a = null;
  private PathData item1aa = null;
  private PathData item1b = null;
  private PathData item2 = null;
  private PathData item3 = null;
  private PathData item4 = null;
  private PathData item5 = null;
  private PathData item5a = null;
  private PathData item5b = null;
  private PathData item5c = null;
  private PathData item5ca = null;
  private PathData item5d = null;
  private PathData item5e = null;

  private LinkedList<PathData> createDirectories() throws IOException {
    item1 = createPathData("item1");
    item1a = createPathData("item1/item1a");
    item1aa = createPathData("item1/item1a/item1aa");
    item1b = createPathData("item1/item1b");
    item2 = createPathData("item2");
    item3 = createPathData("item3");
    item4 = createPathData("item4");
    item5 = createPathData("item5");
    item5a = createPathData("item5/item5a");
    item5b = createPathData("item5/item5b");
    item5c = createPathData("item5/item5c");
    item5ca = createPathData("item5/item5c/item5ca");
    item5d = createPathData("item5/item5d");
    item5e = createPathData("item5/item5e");

    LinkedList<PathData> args = new LinkedList<PathData>();

    when(item1.stat.isDirectory()).thenReturn(true);
    when(item1a.stat.isDirectory()).thenReturn(true);
    when(item1aa.stat.isDirectory()).thenReturn(false);
    when(item1b.stat.isDirectory()).thenReturn(false);
    when(item2.stat.isDirectory()).thenReturn(true);
    when(item3.stat.isDirectory()).thenReturn(false);
    when(item4.stat.isDirectory()).thenReturn(false);
    when(item5.stat.isDirectory()).thenReturn(true);
    when(item5a.stat.isDirectory()).thenReturn(false);
    when(item5b.stat.isDirectory()).thenReturn(false);
    when(item5c.stat.isDirectory()).thenReturn(true);
    when(item5ca.stat.isDirectory()).thenReturn(false);
    when(item5d.stat.isDirectory()).thenReturn(false);
    when(item5e.stat.isDirectory()).thenReturn(false);

    when(mockFs.listStatus(eq(item1.path))).thenReturn(
        new FileStatus[] { item1a.stat, item1b.stat });
    when(mockFs.listStatus(eq(item1a.path))).thenReturn(
        new FileStatus[] { item1aa.stat });
    when(mockFs.listStatus(eq(item2.path))).thenReturn(new FileStatus[0]);
    when(mockFs.listStatus(eq(item5.path))).thenReturn(
        new FileStatus[] { item5a.stat, item5b.stat, item5c.stat, item5d.stat,
            item5e.stat });
    when(mockFs.listStatus(eq(item5c.path))).thenReturn(
        new FileStatus[] { item5ca.stat });

    when(mockFs.listStatusIterator(Mockito.any(Path.class)))
        .thenAnswer(new Answer<RemoteIterator<FileStatus>>() {

          @Override
          public RemoteIterator<FileStatus> answer(InvocationOnMock invocation)
              throws Throwable {
            final Path p = (Path) invocation.getArguments()[0];
            final FileStatus[] stats = mockFs.listStatus(p);

            return new RemoteIterator<FileStatus>() {
              private int i = 0;

              @Override
              public boolean hasNext() throws IOException {
                return i < stats.length;
              }

              @Override
              public FileStatus next() throws IOException {
                if (!hasNext()) {
                  throw new NoSuchElementException("No more entry in " + p);
                }
                return stats[i++];
              }
            };
          }
        });

    when(item1.stat.isSymlink()).thenReturn(false);
    when(item1a.stat.isSymlink()).thenReturn(false);
    when(item1aa.stat.isSymlink()).thenReturn(false);
    when(item1b.stat.isSymlink()).thenReturn(false);
    when(item2.stat.isSymlink()).thenReturn(false);
    when(item3.stat.isSymlink()).thenReturn(false);
    when(item4.stat.isSymlink()).thenReturn(true);
    when(item5.stat.isSymlink()).thenReturn(false);
    when(item5a.stat.isSymlink()).thenReturn(true);
    when(item5b.stat.isSymlink()).thenReturn(true);
    when(item5d.stat.isSymlink()).thenReturn(true);
    when(item5e.stat.isSymlink()).thenReturn(true);

    when(item4.stat.getSymlink()).thenReturn(item3.path);
    when(item5a.stat.getSymlink()).thenReturn(item1b.path);
    when(item5b.stat.getSymlink()).thenReturn(item5.path);
    when(item5d.stat.getSymlink()).thenReturn(item5c.path);
    when(item5e.stat.getSymlink()).thenReturn(item5ca.path);

    args.add(item1);
    args.add(item2);
    args.add(item3);
    args.add(item4);
    args.add(item5);

    return args;
  }

  private PathData createPathData(String name) throws IOException {
    Path path = new Path(name);
    FileStatus fstat = mock(FileStatus.class);
    when(fstat.getPath()).thenReturn(path);
    when(fstat.toString()).thenReturn("fileStatus:" + name);

    when(mockFs.getFileStatus(eq(path))).thenReturn(fstat);
    PathData item = new PathData(path.toString(), conf);
    return item;
  }

  private LinkedList<String> getArgs(String cmd) {
    return new LinkedList<String>(Arrays.asList(cmd.split(" ")));
  }
}
