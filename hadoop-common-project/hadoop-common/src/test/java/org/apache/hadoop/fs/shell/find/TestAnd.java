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
import java.util.Deque;
import java.util.LinkedList;

import org.apache.hadoop.fs.shell.PathData;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.junit.Test;

public class TestAnd {

  @Rule
  public Timeout globalTimeout = new Timeout(10000);

  // test all expressions passing
  @Test
  public void testPass() throws IOException {
    And and = new And();

    PathData pathData = mock(PathData.class);

    Expression first = mock(Expression.class);
    when(first.apply(pathData, -1)).thenReturn(Result.PASS);

    Expression second = mock(Expression.class);
    when(second.apply(pathData, -1)).thenReturn(Result.PASS);

    Deque<Expression> children = new LinkedList<Expression>();
    children.add(second);
    children.add(first);
    and.addChildren(children);

    assertEquals(Result.PASS, and.apply(pathData, -1));
    verify(first).apply(pathData, -1);
    verify(second).apply(pathData, -1);
    verifyNoMoreInteractions(first);
    verifyNoMoreInteractions(second);
  }

  // test the first expression failing
  @Test
  public void testFailFirst() throws IOException {
    And and = new And();

    PathData pathData = mock(PathData.class);

    Expression first = mock(Expression.class);
    when(first.apply(pathData, -1)).thenReturn(Result.FAIL);

    Expression second = mock(Expression.class);
    when(second.apply(pathData, -1)).thenReturn(Result.PASS);

    Deque<Expression> children = new LinkedList<Expression>();
    children.add(second);
    children.add(first);
    and.addChildren(children);

    assertEquals(Result.FAIL, and.apply(pathData, -1));
    verify(first).apply(pathData, -1);
    verifyNoMoreInteractions(first);
    verifyNoMoreInteractions(second);
  }

  // test the second expression failing
  @Test
  public void testFailSecond() throws IOException {
    And and = new And();

    PathData pathData = mock(PathData.class);

    Expression first = mock(Expression.class);
    when(first.apply(pathData, -1)).thenReturn(Result.PASS);

    Expression second = mock(Expression.class);
    when(second.apply(pathData, -1)).thenReturn(Result.FAIL);

    Deque<Expression> children = new LinkedList<Expression>();
    children.add(second);
    children.add(first);
    and.addChildren(children);

    assertEquals(Result.FAIL, and.apply(pathData, -1));
    verify(first).apply(pathData, -1);
    verify(second).apply(pathData, -1);
    verifyNoMoreInteractions(first);
    verifyNoMoreInteractions(second);
  }

  // test both expressions failing
  @Test
  public void testFailBoth() throws IOException {
    And and = new And();

    PathData pathData = mock(PathData.class);

    Expression first = mock(Expression.class);
    when(first.apply(pathData, -1)).thenReturn(Result.FAIL);

    Expression second = mock(Expression.class);
    when(second.apply(pathData, -1)).thenReturn(Result.FAIL);

    Deque<Expression> children = new LinkedList<Expression>();
    children.add(second);
    children.add(first);
    and.addChildren(children);

    assertEquals(Result.FAIL, and.apply(pathData, -1));
    verify(first).apply(pathData, -1);
    verifyNoMoreInteractions(first);
    verifyNoMoreInteractions(second);
  }

  // test the first expression stopping
  @Test
  public void testStopFirst() throws IOException {
    And and = new And();

    PathData pathData = mock(PathData.class);

    Expression first = mock(Expression.class);
    when(first.apply(pathData, -1)).thenReturn(Result.STOP);

    Expression second = mock(Expression.class);
    when(second.apply(pathData, -1)).thenReturn(Result.PASS);

    Deque<Expression> children = new LinkedList<Expression>();
    children.add(second);
    children.add(first);
    and.addChildren(children);

    assertEquals(Result.STOP, and.apply(pathData, -1));
    verify(first).apply(pathData, -1);
    verify(second).apply(pathData, -1);
    verifyNoMoreInteractions(first);
    verifyNoMoreInteractions(second);
  }

  // test the second expression stopping
  @Test
  public void testStopSecond() throws IOException {
    And and = new And();

    PathData pathData = mock(PathData.class);

    Expression first = mock(Expression.class);
    when(first.apply(pathData, -1)).thenReturn(Result.PASS);

    Expression second = mock(Expression.class);
    when(second.apply(pathData, -1)).thenReturn(Result.STOP);

    Deque<Expression> children = new LinkedList<Expression>();
    children.add(second);
    children.add(first);
    and.addChildren(children);

    assertEquals(Result.STOP, and.apply(pathData, -1));
    verify(first).apply(pathData, -1);
    verify(second).apply(pathData, -1);
    verifyNoMoreInteractions(first);
    verifyNoMoreInteractions(second);
  }

  // test first expression stopping and second failing
  @Test
  public void testStopFail() throws IOException {
    And and = new And();

    PathData pathData = mock(PathData.class);

    Expression first = mock(Expression.class);
    when(first.apply(pathData, -1)).thenReturn(Result.STOP);

    Expression second = mock(Expression.class);
    when(second.apply(pathData, -1)).thenReturn(Result.FAIL);

    Deque<Expression> children = new LinkedList<Expression>();
    children.add(second);
    children.add(first);
    and.addChildren(children);

    assertEquals(Result.STOP.combine(Result.FAIL), and.apply(pathData, -1));
    verify(first).apply(pathData, -1);
    verify(second).apply(pathData, -1);
    verifyNoMoreInteractions(first);
    verifyNoMoreInteractions(second);
  }

  // test setOptions is called on child
  @Test
  public void testSetOptions() throws IOException {
    And and = new And();
    Expression first = mock(Expression.class);
    Expression second = mock(Expression.class);

    Deque<Expression> children = new LinkedList<Expression>();
    children.add(second);
    children.add(first);
    and.addChildren(children);

    FindOptions options = mock(FindOptions.class);
    and.setOptions(options);
    verify(first).setOptions(options);
    verify(second).setOptions(options);
    verifyNoMoreInteractions(first);
    verifyNoMoreInteractions(second);
  }

  // test prepare is called on child
  @Test
  public void testPrepare() throws IOException {
    And and = new And();
    Expression first = mock(Expression.class);
    Expression second = mock(Expression.class);

    Deque<Expression> children = new LinkedList<Expression>();
    children.add(second);
    children.add(first);
    and.addChildren(children);

    and.prepare();
    verify(first).prepare();
    verify(second).prepare();
    verifyNoMoreInteractions(first);
    verifyNoMoreInteractions(second);
  }

  // test finish is called on child
  @Test
  public void testFinish() throws IOException {
    And and = new And();
    Expression first = mock(Expression.class);
    Expression second = mock(Expression.class);

    Deque<Expression> children = new LinkedList<Expression>();
    children.add(second);
    children.add(first);
    and.addChildren(children);

    and.finish();
    verify(first).finish();
    verify(second).finish();
    verifyNoMoreInteractions(first);
    verifyNoMoreInteractions(second);
  }
}
