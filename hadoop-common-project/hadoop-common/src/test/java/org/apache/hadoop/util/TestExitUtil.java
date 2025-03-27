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

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.util.ExitUtil.ExitException;
import org.apache.hadoop.util.ExitUtil.HaltException;
import org.apache.hadoop.test.AbstractHadoopTestBase;

public class TestExitUtil extends AbstractHadoopTestBase {

  @BeforeEach
  public void before() {
    ExitUtil.disableSystemExit();
    ExitUtil.disableSystemHalt();
    ExitUtil.resetFirstExitException();
    ExitUtil.resetFirstHaltException();
  }

  @AfterEach
  public void after() {
    ExitUtil.resetFirstExitException();
    ExitUtil.resetFirstHaltException();
  }

  @Test
  public void testGetSetExitExceptions() throws Throwable {
    // prepare states and exceptions
    ExitException ee1 = new ExitException(1, "TestExitUtil forged 1st ExitException");
    ExitException ee2 = new ExitException(2, "TestExitUtil forged 2nd ExitException");
    // check proper initial settings
    assertFalse(ExitUtil.terminateCalled(),
        "ExitUtil.terminateCalled initial value should be false");
    assertNull(ExitUtil.getFirstExitException(),
        "ExitUtil.getFirstExitException initial value should be null");

    // simulate/check 1st call
    ExitException ee = intercept(ExitException.class, ()->ExitUtil.terminate(ee1));
    assertSame(ee1, ee, "ExitUtil.terminate should have rethrown its ExitException argument but it "
        + "had thrown something else");
    assertTrue(ExitUtil.terminateCalled(),
        "ExitUtil.terminateCalled should be true after 1st ExitUtil.terminate call");
    assertSame(ee1, ExitUtil.getFirstExitException(),
        "ExitUtil.terminate should store its 1st call's ExitException");

    // simulate/check 2nd call not overwritting 1st one
    ee = intercept(ExitException.class, ()->ExitUtil.terminate(ee2));
    assertSame(ee2, ee,
        "ExitUtil.terminate should have rethrown its HaltException argument but it "
        + "had thrown something else");
    assertTrue(ExitUtil.terminateCalled(),
        "ExitUtil.terminateCalled should still be true after 2nd ExitUtil.terminate call");
    // 2nd call rethrown the 2nd ExitException yet only the 1st only should have been stored
    assertSame(ee1, ExitUtil.getFirstExitException(),
        "ExitUtil.terminate when called twice should only remember 1st call's "
        + "ExitException");

    // simulate cleanup, also tries to make sure state is ok for all junit still has to do
    ExitUtil.resetFirstExitException();
    assertFalse(ExitUtil.terminateCalled(), "ExitUtil.terminateCalled should be false after "
        + "ExitUtil.resetFirstExitException call");
    assertNull(ExitUtil.getFirstExitException(),
        "ExitUtil.getFirstExitException should be null after "
        + "ExitUtil.resetFirstExitException call");
  }

  @Test
  public void testGetSetHaltExceptions() throws Throwable {
    // prepare states and exceptions
    ExitUtil.disableSystemHalt();
    ExitUtil.resetFirstHaltException();
    HaltException he1 = new HaltException(1, "TestExitUtil forged 1st HaltException");
    HaltException he2 = new HaltException(2, "TestExitUtil forged 2nd HaltException");

    // check proper initial settings
    assertFalse(ExitUtil.haltCalled(),
        "ExitUtil.haltCalled initial value should be false");
    assertNull(ExitUtil.getFirstHaltException(),
        "ExitUtil.getFirstHaltException initial value should be null");

    // simulate/check 1st call
    HaltException he = intercept(HaltException.class, ()->ExitUtil.halt(he1));
    assertSame(he1, he, "ExitUtil.halt should have rethrown its HaltException argument but it had "
        +"thrown something else");
    assertTrue(ExitUtil.haltCalled(),
        "ExitUtil.haltCalled should be true after 1st ExitUtil.halt call");
    assertSame(he1, ExitUtil.getFirstHaltException(),
        "ExitUtil.halt should store its 1st call's HaltException");

    // simulate/check 2nd call not overwritting 1st one
    he = intercept(HaltException.class, ()->ExitUtil.halt(he2));
    assertSame(he2, he, "ExitUtil.halt should have rethrown its HaltException argument but it had "
        + "thrown something else");
    assertTrue(ExitUtil.haltCalled(),
        "ExitUtil.haltCalled should still be true after 2nd ExitUtil.halt call");
    assertSame(he1, ExitUtil.getFirstHaltException(),
        "ExitUtil.halt when called twice should only remember 1st call's HaltException");

    // simulate cleanup, also tries to make sure state is ok for all junit still has to do
    ExitUtil.resetFirstHaltException();
    assertFalse(ExitUtil.haltCalled(), "ExitUtil.haltCalled should be false after "
        + "ExitUtil.resetFirstHaltException call");
    assertNull(ExitUtil.getFirstHaltException(),
        "ExitUtil.getFirstHaltException should be null after "
        + "ExitUtil.resetFirstHaltException call");
  }
}
