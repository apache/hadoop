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
package org.apache.hadoop.fs;


import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.fs.shell.CommandFormat.NotEnoughArgumentsException;
import org.apache.hadoop.fs.shell.CommandFormat.TooManyArgumentsException;
import org.apache.hadoop.fs.shell.CommandFormat.UnknownOptionException;
import org.junit.Before;
import org.junit.Test;

/**
 * This class tests the command line parsing
 */
public class TestCommandFormat {
  private static List<String> args;
  private static List<String> expectedArgs;
  private static Set<String> expectedOpts;
  
  @Before
  public void setUp() {
    args = new ArrayList<String>();
    expectedOpts = new HashSet<String>();
    expectedArgs = new ArrayList<String>();
  }

  @Test
  public void testNoArgs() {
    checkArgLimits(null, 0, 0);
    checkArgLimits(null, 0, 1);    
    checkArgLimits(NotEnoughArgumentsException.class, 1, 1);
    checkArgLimits(NotEnoughArgumentsException.class, 1, 2);
  }
  
  @Test
  public void testOneArg() {
    args = listOf("a");
    expectedArgs = listOf("a");

    checkArgLimits(TooManyArgumentsException.class, 0, 0);
    checkArgLimits(null, 0, 1);    
    checkArgLimits(null, 1, 1);
    checkArgLimits(null, 1, 2);
    checkArgLimits(NotEnoughArgumentsException.class, 2, 3);
  }
  
  @Test
  public void testTwoArgs() {
    args = listOf("a", "b");
    expectedArgs = listOf("a", "b");

    checkArgLimits(TooManyArgumentsException.class, 0, 0);
    checkArgLimits(TooManyArgumentsException.class, 1, 1);
    checkArgLimits(null, 1, 2);
    checkArgLimits(null, 2, 2);
    checkArgLimits(null, 2, 3);
    checkArgLimits(NotEnoughArgumentsException.class, 3, 3);
  }

  @Test
  public void testOneOpt() {
    args = listOf("-a");
    expectedOpts = setOf("a");
    
    checkArgLimits(UnknownOptionException.class, 0, 0);
    checkArgLimits(null, 0, 0, "a", "b");
    checkArgLimits(NotEnoughArgumentsException.class, 1, 1, "a", "b");
  }
  
  @Test
  public void testTwoOpts() {
    args = listOf("-a", "-b");
    expectedOpts = setOf("a", "b");
    
    checkArgLimits(UnknownOptionException.class, 0, 0);
    checkArgLimits(null, 0, 0, "a", "b");
    checkArgLimits(null, 0, 1, "a", "b");
    checkArgLimits(NotEnoughArgumentsException.class, 1, 1, "a", "b");
  }
  
  @Test
  public void testOptArg() {
    args = listOf("-a", "b");
    expectedOpts = setOf("a");
    expectedArgs = listOf("b");

    checkArgLimits(UnknownOptionException.class, 0, 0);
    checkArgLimits(TooManyArgumentsException.class, 0, 0, "a", "b");
    checkArgLimits(null, 0, 1, "a", "b");
    checkArgLimits(null, 1, 1, "a", "b");
    checkArgLimits(null, 1, 2, "a", "b");
    checkArgLimits(NotEnoughArgumentsException.class, 2, 2, "a", "b");
  }

  @Test
  public void testArgOpt() {
    args = listOf("b", "-a");
    expectedArgs = listOf("b", "-a");

    checkArgLimits(TooManyArgumentsException.class, 0, 0, "a", "b");
    checkArgLimits(null, 1, 2, "a", "b");
    checkArgLimits(null, 2, 2, "a", "b");
    checkArgLimits(NotEnoughArgumentsException.class, 3, 4, "a", "b");
  }

  @Test
  public void testOptStopOptArg() {
    args = listOf("-a", "--", "-b", "c");
    expectedOpts = setOf("a");
    expectedArgs = listOf("-b", "c");

    checkArgLimits(UnknownOptionException.class, 0, 0);
    checkArgLimits(TooManyArgumentsException.class, 0, 1, "a", "b");
    checkArgLimits(null, 2, 2, "a", "b");
    checkArgLimits(NotEnoughArgumentsException.class, 3, 4, "a", "b");
  }

  @Test
  public void testOptDashArg() {
    args = listOf("-b", "-", "-c");
    expectedOpts = setOf("b");
    expectedArgs = listOf("-", "-c");

    checkArgLimits(UnknownOptionException.class, 0, 0);
    checkArgLimits(TooManyArgumentsException.class, 0, 0, "b", "c");
    checkArgLimits(TooManyArgumentsException.class, 1, 1, "b", "c");
    checkArgLimits(null, 2, 2, "b", "c");
    checkArgLimits(NotEnoughArgumentsException.class, 3, 4, "b", "c");
  }
  
  @Test
  public void testOldArgsWithIndex() {
    String[] arrayArgs = new String[]{"ignore", "-a", "b", "-c"};
    {
      CommandFormat cf = new CommandFormat(0, 9, "a", "c");
      List<String> parsedArgs = cf.parse(arrayArgs, 0);
      assertEquals(setOf(), cf.getOpts());
      assertEquals(listOf("ignore", "-a", "b", "-c"), parsedArgs);
    }
    { 
      CommandFormat cf = new CommandFormat(0, 9, "a", "c");
      List<String> parsedArgs = cf.parse(arrayArgs, 1);
      assertEquals(setOf("a"), cf.getOpts());
      assertEquals(listOf("b", "-c"), parsedArgs);
    }
    { 
      CommandFormat cf = new CommandFormat(0, 9, "a", "c");
      List<String> parsedArgs = cf.parse(arrayArgs, 2);
      assertEquals(setOf(), cf.getOpts());
      assertEquals(listOf("b", "-c"), parsedArgs);
    }
  }
  
  private static <T> CommandFormat checkArgLimits(
      Class<? extends IllegalArgumentException> expectedErr,
      int min, int max, String ... opts)
  {
    CommandFormat cf = new CommandFormat(min, max, opts);
    List<String> parsedArgs = new ArrayList<String>(args);
    
    Class<?> cfError = null;
    try {
      cf.parse(parsedArgs);
    } catch (IllegalArgumentException e) {
      System.out.println(e.getMessage());
      cfError = e.getClass();
    }

    assertEquals(expectedErr, cfError);
    if (expectedErr == null) {
      assertEquals(expectedArgs, parsedArgs);
      assertEquals(expectedOpts, cf.getOpts());
    }
    return cf;
  }
  
  // Don't use generics to avoid warning:
  // unchecked generic array creation of type T[] for varargs parameter
  private static List<String> listOf(String ... objects) {
    return Arrays.asList(objects);
  }
  
  private static Set<String> setOf(String ... objects) {
    return new HashSet<String>(listOf(objects));
  }
}
