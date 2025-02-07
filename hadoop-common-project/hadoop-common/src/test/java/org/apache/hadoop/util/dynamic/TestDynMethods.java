/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.hadoop.util.dynamic;

import java.util.concurrent.Callable;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Copied from {@code org.apache.parquet.util} test suites.
 */
public class TestDynMethods extends AbstractHadoopTestBase {

  @Test
  public void testNoImplCall() throws Exception {
    final DynMethods.Builder builder = new DynMethods.Builder("concat");

    intercept(NoSuchMethodException.class,
        (Callable<DynMethods.UnboundMethod>) builder::buildChecked);

    intercept(RuntimeException.class,
        (Callable<DynMethods.UnboundMethod>) builder::build);
  }

  @Test
  public void testMissingClass() throws Exception {
    final DynMethods.Builder builder = new DynMethods.Builder("concat")
        .impl("not.a.RealClass", String.class, String.class);

    intercept(NoSuchMethodException.class,
        (Callable<DynMethods.UnboundMethod>) builder::buildChecked);

    intercept(RuntimeException.class, () ->
        builder.build());
  }

  @Test
  public void testMissingMethod() throws Exception {
    final DynMethods.Builder builder = new DynMethods.Builder("concat")
        .impl(Concatenator.class, "cat2strings", String.class, String.class);

    intercept(NoSuchMethodException.class,
        (Callable<DynMethods.UnboundMethod>) builder::buildChecked);

    intercept(RuntimeException.class, () ->
        builder.build());

  }

  @Test
  public void testFirstImplReturned() throws Exception {
    Concatenator obj = new Concatenator("-");
    DynMethods.UnboundMethod cat2 = new DynMethods.Builder("concat")
        .impl("not.a.RealClass", String.class, String.class)
        .impl(Concatenator.class, String.class, String.class)
        .impl(Concatenator.class, String.class, String.class, String.class)
        .buildChecked();

    assertEquals("a-b", cat2.invoke(obj, "a", "b"),
        "Should call the 2-arg version successfully");

    assertEquals("a-b", cat2.invoke(obj, "a", "b", "c"),
        "Should ignore extra arguments");

    DynMethods.UnboundMethod cat3 = new DynMethods.Builder("concat")
        .impl("not.a.RealClass", String.class, String.class)
        .impl(Concatenator.class, String.class, String.class, String.class)
        .impl(Concatenator.class, String.class, String.class)
        .build();

    assertEquals("a-b-c", cat3.invoke(obj, "a", "b", "c"),
        "Should call the 3-arg version successfully");

    assertEquals("a-b-null", cat3.invoke(obj, "a", "b"),
        "Should call the 3-arg version null padding");
  }

  @Test
  public void testVarArgs() throws Exception {
    DynMethods.UnboundMethod cat = new DynMethods.Builder("concat")
        .impl(Concatenator.class, String[].class)
        .buildChecked();

    assertEquals("abcde",
        cat.invokeChecked(new Concatenator(), (Object) new String[]{"a", "b", "c", "d", "e"}),
        "Should use the varargs version");

    assertEquals("abcde",
        cat.bind(new Concatenator())
        .invokeChecked((Object) new String[]{"a", "b", "c", "d", "e"}),
        "Should use the varargs version");
  }

  @Test
  public void testIncorrectArguments() throws Exception {
    final Concatenator obj = new Concatenator("-");
    final DynMethods.UnboundMethod cat = new DynMethods.Builder("concat")
        .impl("not.a.RealClass", String.class, String.class)
        .impl(Concatenator.class, String.class, String.class)
        .buildChecked();

    intercept(IllegalArgumentException.class, () ->
        cat.invoke(obj, 3, 4));

    intercept(IllegalArgumentException.class, () ->
        cat.invokeChecked(obj, 3, 4));
  }

  @Test
  public void testExceptionThrown() throws Exception {
    final Concatenator.SomeCheckedException exc = new Concatenator.SomeCheckedException();
    final Concatenator obj = new Concatenator("-");
    final DynMethods.UnboundMethod cat = new DynMethods.Builder("concat")
        .impl("not.a.RealClass", String.class, String.class)
        .impl(Concatenator.class, Exception.class)
        .buildChecked();

    intercept(Concatenator.SomeCheckedException.class, () ->
        cat.invokeChecked(obj, exc));

    intercept(RuntimeException.class, () ->
        cat.invoke(obj, exc));
  }

  @Test
  public void testNameChange() throws Exception {
    Concatenator obj = new Concatenator("-");
    DynMethods.UnboundMethod cat = new DynMethods.Builder("cat")
        .impl(Concatenator.class, "concat", String.class, String.class)
        .buildChecked();

    assertEquals("a-b", cat.invoke(obj, "a", "b"),
        "Should find 2-arg concat method");
  }

  @Test
  public void testStringClassname() throws Exception {
    Concatenator obj = new Concatenator("-");
    DynMethods.UnboundMethod cat = new DynMethods.Builder("concat")
        .impl(Concatenator.class.getName(), String.class, String.class)
        .buildChecked();

    assertEquals("a-b", cat.invoke(obj, "a", "b"),
        "Should find 2-arg concat method");
  }

  @Test
  public void testHiddenMethod() throws Exception {
    Concatenator obj = new Concatenator("-");

    intercept(NoSuchMethodException.class, () ->
        new DynMethods.Builder("setSeparator")
            .impl(Concatenator.class, String.class)
            .buildChecked());

    DynMethods.UnboundMethod changeSep = new DynMethods.Builder("setSeparator")
        .hiddenImpl(Concatenator.class, String.class)
        .buildChecked();

    assertNotNull(changeSep, "Should find hidden method with hiddenImpl");

    changeSep.invokeChecked(obj, "/");

    assertEquals("a/b", obj.concat("a", "b"),
        "Should use separator / instead of -");
  }

  @Test
  public void testBoundMethod() throws Exception {
    DynMethods.UnboundMethod cat = new DynMethods.Builder("concat")
        .impl(Concatenator.class, String.class, String.class)
        .buildChecked();

    // Unbound methods can be bound multiple times
    DynMethods.BoundMethod dashCat = cat.bind(new Concatenator("-"));
    DynMethods.BoundMethod underCat = cat.bind(new Concatenator("_"));

    assertEquals("a-b", dashCat.invoke("a", "b"),
        "Should use '-' object without passing");
    assertEquals("a_b", underCat.invoke("a", "b"),
        "Should use '_' object without passing");

    DynMethods.BoundMethod slashCat = new DynMethods.Builder("concat")
        .impl(Concatenator.class, String.class, String.class)
        .buildChecked(new Concatenator("/"));

    assertEquals("a/b", slashCat.invoke("a", "b"),
        "Should use bound object from builder without passing");
  }

  @Test
  public void testBindStaticMethod() throws Exception {
    final DynMethods.Builder builder = new DynMethods.Builder("cat")
        .impl(Concatenator.class, String[].class);

    intercept(IllegalStateException.class, () ->
        builder.buildChecked(new Concatenator()));

    intercept(IllegalStateException.class, () ->
        builder.build(new Concatenator()));

    final DynMethods.UnboundMethod staticCat = builder.buildChecked();
    assertTrue(staticCat.isStatic(), "Should be static");

    intercept(IllegalStateException.class, () ->
        staticCat.bind(new Concatenator()));
  }

  @Test
  public void testStaticMethod() throws Exception {
    DynMethods.StaticMethod staticCat = new DynMethods.Builder("cat")
        .impl(Concatenator.class, String[].class)
        .buildStaticChecked();

    assertEquals("abcde", staticCat.invokeChecked(
        (Object) new String[]{"a", "b", "c", "d", "e"}),
        "Should call varargs static method cat(String...)");
  }

  @Test
  public void testNonStaticMethod() throws Exception {
    final DynMethods.Builder builder = new DynMethods.Builder("concat")
        .impl(Concatenator.class, String.class, String.class);

    intercept(IllegalStateException.class, builder::buildStatic);

    intercept(IllegalStateException.class, builder::buildStaticChecked);

    final DynMethods.UnboundMethod cat2 = builder.buildChecked();
    assertFalse(cat2.isStatic(),
        "concat(String,String) should not be static");

    intercept(IllegalStateException.class, cat2::asStatic);
  }

  @Test
  public void testConstructorImpl() throws Exception {
    final DynMethods.Builder builder = new DynMethods.Builder("newConcatenator")
        .ctorImpl(Concatenator.class, String.class)
        .impl(Concatenator.class, String.class);

    DynMethods.UnboundMethod newConcatenator = builder.buildChecked();
    assertTrue(newConcatenator instanceof DynConstructors.Ctor,
        "Should find constructor implementation");
    assertTrue(newConcatenator.isStatic(),
        "Constructor should be a static method");
    assertFalse(newConcatenator.isNoop(), "Constructor should not be NOOP");

    // constructors cannot be bound
    intercept(IllegalStateException.class, () ->
        builder.buildChecked(new Concatenator()));
    intercept(IllegalStateException.class, () ->
        builder.build(new Concatenator()));

    Concatenator concatenator = newConcatenator.asStatic().invoke("*");
    assertEquals("a*b", concatenator.concat("a", "b"),
        "Should function as a concatenator");

    concatenator = newConcatenator.asStatic().invokeChecked("@");
    assertEquals("a@b", concatenator.concat("a", "b"),
        "Should function as a concatenator");
  }

  @Test
  public void testConstructorImplAfterFactoryMethod() throws Exception {
    DynMethods.UnboundMethod newConcatenator = new DynMethods.Builder("newConcatenator")
        .impl(Concatenator.class, String.class)
        .ctorImpl(Concatenator.class, String.class)
        .buildChecked();

    assertFalse(newConcatenator instanceof DynConstructors.Ctor,
        "Should find factory method before constructor method");
  }

  @Test
  public void testNoop() throws Exception {
    // noop can be unbound, bound, or static
    DynMethods.UnboundMethod noop = new DynMethods.Builder("concat")
        .impl("not.a.RealClass", String.class, String.class)
        .orNoop()
        .buildChecked();

    assertTrue(noop.isNoop(), "No implementation found, should return NOOP");
    assertNull(noop.invoke(new Concatenator(), "a"),
        "NOOP should always return null");
    assertNull(noop.invoke(null, "a"), "NOOP can be called with null");
    assertNull(noop.bind(new Concatenator()).invoke("a"), "NOOP can be bound");
    assertNull(noop.bind(null).invoke("a"), "NOOP can be bound to null");
    assertNull(noop.asStatic().invoke("a"), "NOOP can be static");
  }
}
