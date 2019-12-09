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

package org.apache.hadoop.util;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;

public class TestGenericsUtil {

  @Test
  public void testToArray() {

    //test a list of size 10
    List<Integer> list = new ArrayList<Integer>();

    for(int i=0; i<10; i++) {
      list.add(i);
    }

    Integer[] arr = GenericsUtil.toArray(list);

    for (int i = 0; i < arr.length; i++) {
      assertEquals(
              "Array has identical elements as input list",
              list.get(i), arr[i]);
    }
  }

  @Test
  public void testWithEmptyList() {
    try {
      List<String> list = new ArrayList<String>();
      String[] arr = GenericsUtil.toArray(list);
      fail("Empty array should throw exception");
      System.out.println(arr); //use arr so that compiler will not complain

    }catch (IndexOutOfBoundsException ex) {
      //test case is successful
    }
  }

  @Test
  public void testWithEmptyList2() {
    List<String> list = new ArrayList<String>();
    //this method should not throw IndexOutOfBoundsException
    String[] arr = GenericsUtil.<String>toArray(String.class, list);

    assertEquals("Assert list creation w/ no elements results in length 0",
            0, arr.length);
  }

  /** This class uses generics */
  private class GenericClass<T> {
    T dummy;
    List<T> list = new ArrayList<T>();

    void add(T item) {
      list.add(item);
    }

    T[] funcThatUsesToArray() {
      T[] arr = GenericsUtil.toArray(list);
      return arr;
    }
  }

  @Test
  public void testWithGenericClass() {

    GenericClass<String> testSubject = new GenericClass<String>();

    testSubject.add("test1");
    testSubject.add("test2");

    try {
      //this cast would fail, if we had not used GenericsUtil.toArray, since the
      //rmethod would return Object[] rather than String[]
      String[] arr = testSubject.funcThatUsesToArray();

      assertEquals("test1", arr[0]);
      assertEquals("test2", arr[1]);

    }catch (ClassCastException ex) {
      fail("GenericsUtil#toArray() is not working for generic classes");
    }

  }

  @Test
  public void testGenericOptionsParser() throws Exception {
     GenericOptionsParser parser = new GenericOptionsParser(
        new Configuration(), new String[] {"-jt"});
    assertEquals(0, parser.getRemainingArgs().length);

    //  test if -D accepts -Dx=y=z
    parser =
      new GenericOptionsParser(new Configuration(),
                               new String[] {"-Dx=y=z"});
    assertEquals(
            "Options parser gets entire ='s expresion",
            "y=z", parser.getConfiguration().get("x"));
  }

  @Test
  public void testGetClass() {

    //test with Integer
    Integer x = new Integer(42);
    Class<Integer> c = GenericsUtil.getClass(x);
    assertEquals("Correct generic type is acquired from object",
            Integer.class, c);

    //test with GenericClass<Integer>
    GenericClass<Integer> testSubject = new GenericClass<Integer>();
    Class<GenericClass<Integer>> c2 = GenericsUtil.getClass(testSubject);
    assertEquals("Inner generics are acquired from object.",
            GenericClass.class, c2);
  }

  @Test
  public void testIsLog4jLogger() throws Exception {
    assertFalse("False if clazz is null", GenericsUtil.isLog4jLogger(null));
    assertTrue("The implementation is Log4j",
        GenericsUtil.isLog4jLogger(TestGenericsUtil.class));
  }
}
