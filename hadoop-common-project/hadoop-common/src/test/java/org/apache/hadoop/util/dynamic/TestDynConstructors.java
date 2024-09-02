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

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Derived from {@code org.apache.parquet.util} test suites.
 */
public class TestDynConstructors extends AbstractHadoopTestBase {

  @Test
  public void testNoImplCall() throws Exception {
    final DynConstructors.Builder builder = new DynConstructors.Builder();

    intercept(NoSuchMethodException.class,
        (Callable<DynMethods.UnboundMethod>) builder::buildChecked);

    intercept(RuntimeException.class, () ->
        builder.build());
  }

  @Test
  public void testMissingClass() throws Exception {
    final DynConstructors.Builder builder = new DynConstructors.Builder()
        .impl("not.a.RealClass");

    intercept(NoSuchMethodException.class,
        (Callable<DynMethods.UnboundMethod>) builder::buildChecked);

    intercept(RuntimeException.class, (Callable<DynMethods.UnboundMethod>) builder::build);
  }

  @Test
  public void testMissingConstructor() throws Exception {
    final DynConstructors.Builder builder = new DynConstructors.Builder()
        .impl(Concatenator.class, String.class, String.class);

    intercept(NoSuchMethodException.class,
        (Callable<DynMethods.UnboundMethod>) builder::buildChecked);

    intercept(RuntimeException.class,
        (Callable<DynMethods.UnboundMethod>) builder::build);
  }

  @Test
  public void testFirstImplReturned() throws Exception {
    final DynConstructors.Ctor<Concatenator> sepCtor = new DynConstructors.Builder()
        .impl("not.a.RealClass", String.class)
        .impl(Concatenator.class, String.class)
        .impl(Concatenator.class)
        .buildChecked();

    Concatenator dashCat = sepCtor.newInstanceChecked("-");
    Assert.assertEquals("Should construct with the 1-arg version",
        "a-b", dashCat.concat("a", "b"));

    intercept(IllegalArgumentException.class, () ->
        sepCtor.newInstanceChecked("/", "-"));

    intercept(IllegalArgumentException.class, () ->
        sepCtor.newInstance("/", "-"));

    DynConstructors.Ctor<Concatenator> defaultCtor = new DynConstructors.Builder()
        .impl("not.a.RealClass", String.class)
        .impl(Concatenator.class)
        .impl(Concatenator.class, String.class)
        .buildChecked();

    Concatenator cat = defaultCtor.newInstanceChecked();
    Assert.assertEquals("Should construct with the no-arg version",
        "ab", cat.concat("a", "b"));
  }

  @Test
  public void testExceptionThrown() throws Exception {
    final Concatenator.SomeCheckedException exc = new Concatenator.SomeCheckedException();
    final DynConstructors.Ctor<Concatenator> sepCtor = new DynConstructors.Builder()
        .impl("not.a.RealClass", String.class)
        .impl(Concatenator.class, Exception.class)
        .buildChecked();

    intercept(Concatenator.SomeCheckedException.class, () ->
        sepCtor.newInstanceChecked(exc));

    intercept(RuntimeException.class, () -> sepCtor.newInstance(exc));
  }

  @Test
  public void testStringClassname() throws Exception {
    final DynConstructors.Ctor<Concatenator> sepCtor = new DynConstructors.Builder()
        .impl(Concatenator.class.getName(), String.class)
        .buildChecked();

    Assert.assertNotNull("Should find 1-arg constructor", sepCtor.newInstance("-"));
  }

  @Test
  public void testHiddenMethod() throws Exception {
    intercept(NoSuchMethodException.class, () ->
        new DynMethods.Builder("setSeparator")
            .impl(Concatenator.class, char.class)
            .buildChecked());

    final DynConstructors.Ctor<Concatenator> sepCtor = new DynConstructors.Builder()
        .hiddenImpl(Concatenator.class.getName(), char.class)
        .buildChecked();

    Assert.assertNotNull("Should find hidden ctor with hiddenImpl", sepCtor);

    Concatenator slashCat = sepCtor.newInstanceChecked('/');

    Assert.assertEquals("Should use separator /",
        "a/b", slashCat.concat("a", "b"));
  }

  @Test
  public void testBind() throws Exception {
    final DynConstructors.Ctor<Concatenator> ctor = new DynConstructors.Builder()
        .impl(Concatenator.class.getName())
        .buildChecked();

    Assert.assertTrue("Should always be static", ctor.isStatic());

    intercept(IllegalStateException.class, () ->
        ctor.bind(null));
  }

  @Test
  public void testInvoke() throws Exception {
    final DynMethods.UnboundMethod ctor = new DynConstructors.Builder()
        .impl(Concatenator.class.getName())
        .buildChecked();

    intercept(IllegalArgumentException.class, () ->
        ctor.invokeChecked("a"));

    intercept(IllegalArgumentException.class, () ->
        ctor.invoke("a"));

    Assert.assertNotNull("Should allow invokeChecked(null, ...)",
        ctor.invokeChecked(null));
    Assert.assertNotNull("Should allow invoke(null, ...)",
        ctor.invoke(null));
  }
}
