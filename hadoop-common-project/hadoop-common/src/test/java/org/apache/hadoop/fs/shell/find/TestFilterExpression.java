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

import org.apache.hadoop.fs.shell.PathData;

import org.junit.Before;
import org.junit.Test;

public class TestFilterExpression {
  private Expression expr;
  private FilterExpression test;

  @Before
  public void setup() {
    expr = mock(Expression.class);
    test = new FilterExpression(expr) {
    };
  }

  // test that the child expression is correctly set
  @Test(timeout = 1000)
  public void expression() throws IOException {
    assertEquals(expr, test.expression);
  }

  // test that setOptions method is called
  @Test(timeout = 1000)
  public void setOptions() throws IOException {
    FindOptions options = mock(FindOptions.class);
    test.setOptions(options);
    verify(expr).setOptions(options);
    verifyNoMoreInteractions(expr);
  }

  // test the apply method is called and the result returned
  @Test(timeout = 1000)
  public void apply() throws IOException {
    PathData item = mock(PathData.class);
    when(expr.apply(item, -1)).thenReturn(Result.PASS).thenReturn(Result.FAIL);
    assertEquals(Result.PASS, test.apply(item, -1));
    assertEquals(Result.FAIL, test.apply(item, -1));
    verify(expr, times(2)).apply(item, -1);
    verifyNoMoreInteractions(expr);
  }

  // test that the finish method is called
  @Test(timeout = 1000)
  public void finish() throws IOException {
    test.finish();
    verify(expr).finish();
    verifyNoMoreInteractions(expr);
  }

  // test that the getUsage method is called
  @Test(timeout = 1000)
  public void getUsage() {
    String[] usage = new String[] { "Usage 1", "Usage 2", "Usage 3" };
    when(expr.getUsage()).thenReturn(usage);
    assertArrayEquals(usage, test.getUsage());
    verify(expr).getUsage();
    verifyNoMoreInteractions(expr);
  }

  // test that the getHelp method is called
  @Test(timeout = 1000)
  public void getHelp() {
    String[] help = new String[] { "Help 1", "Help 2", "Help 3" };
    when(expr.getHelp()).thenReturn(help);
    assertArrayEquals(help, test.getHelp());
    verify(expr).getHelp();
    verifyNoMoreInteractions(expr);
  }

  // test that the isAction method is called
  @Test(timeout = 1000)
  public void isAction() {
    when(expr.isAction()).thenReturn(true).thenReturn(false);
    assertTrue(test.isAction());
    assertFalse(test.isAction());
    verify(expr, times(2)).isAction();
    verifyNoMoreInteractions(expr);
  }

  // test that the isOperator method is called
  @Test(timeout = 1000)
  public void isOperator() {
    when(expr.isAction()).thenReturn(true).thenReturn(false);
    assertTrue(test.isAction());
    assertFalse(test.isAction());
    verify(expr, times(2)).isAction();
    verifyNoMoreInteractions(expr);
  }

  // test that the getPrecedence method is called
  @Test(timeout = 1000)
  public void getPrecedence() {
    int precedence = 12345;
    when(expr.getPrecedence()).thenReturn(precedence);
    assertEquals(precedence, test.getPrecedence());
    verify(expr).getPrecedence();
    verifyNoMoreInteractions(expr);
  }

  // test that the addChildren method is called
  @Test(timeout = 1000)
  public void addChildren() {
    @SuppressWarnings("unchecked")
    Deque<Expression> expressions = mock(Deque.class);
    test.addChildren(expressions);
    verify(expr).addChildren(expressions);
    verifyNoMoreInteractions(expr);
  }

  // test that the addArguments method is called
  @Test(timeout = 1000)
  public void addArguments() {
    @SuppressWarnings("unchecked")
    Deque<String> args = mock(Deque.class);
    test.addArguments(args);
    verify(expr).addArguments(args);
    verifyNoMoreInteractions(expr);
  }
}
