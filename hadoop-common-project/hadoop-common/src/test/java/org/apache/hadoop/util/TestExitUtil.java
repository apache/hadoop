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

import static org.junit.Assert.*;

import org.apache.hadoop.util.ExitUtil.ExitException;
import org.apache.hadoop.util.ExitUtil.HaltException;

import org.junit.Test;

public class TestExitUtil {

  @Test
  public void testGetSetExitExceptions() {
    // prepare states and exceptions
    ExitUtil.disableSystemExit();
    ExitUtil.resetFirstExitException();
    ExitException ee1 = new ExitException(1, "TestExitUtil forged 1st ExitException");
    ExitException ee2 = new ExitException(2, "TestExitUtil forged 2nd ExitException");
    try {
      // check proper initial settings
      assertFalse(ExitUtil.terminateCalled());
      assertNull(ExitUtil.getFirstExitException());

      // simulate/check 1st call
      try {
        ExitUtil.terminate(ee1);
        fail("ExitUtil.terminate should have rethrown its ExitException argument but it returned");
      } catch (ExitException ee) {
        assertEquals("ExitUtil.terminate should have rethrown its ExitException argument but it "
            +"had thrown something else", ee1, ee);
      }
      assertTrue(ExitUtil.terminateCalled());
      assertEquals("ExitUtil.terminate should store its 1st call's ExitException",
          ee1, ExitUtil.getFirstExitException());

      // simulate/check 2nd call not overwritting 1st one
      try {
        ExitUtil.terminate(ee2);
        fail("ExitUtil.terminate should have rethrown its ExitException argument but it returned");
      } catch (ExitException ee) {
        assertEquals("ExitUtil.terminate should have rethrown its HaltException argument but it "
            +"had thrown something else", ee2, ee);
      }
      assertTrue(ExitUtil.terminateCalled());
      // 2nd call rethrown the 2nd ExitException yet only the 1st only should have been stored
      assertEquals("ExitUtil.terminate when called twice should only remember 1st call's "
          +"ExitException", ee1, ExitUtil.getFirstExitException());

      // simulate cleanup, also tries to make sure state is ok for all junit still has to do
      ExitUtil.resetFirstExitException();
      assertFalse(ExitUtil.terminateCalled());
      assertNull(ExitUtil.getFirstExitException());
    } finally {
      // cleanup
      ExitUtil.resetFirstExitException();
    }
  }

  @Test
  public void testGetSetHaltExceptions() {
    // prepare states and exceptions
    ExitUtil.disableSystemHalt();
    ExitUtil.resetFirstHaltException();
    HaltException he1 = new HaltException(1, "TestExitUtil forged 1st HaltException");
    HaltException he2 = new HaltException(2, "TestExitUtil forged 2nd HaltException");
    try {
      // check proper initial settings
      assertFalse(ExitUtil.haltCalled());
      assertNull(ExitUtil.getFirstHaltException());

      // simulate/check 1st call
      try {
        ExitUtil.halt(he1);
        fail("ExitUtil.halt should have rethrown its HaltException argument but it returned");
      } catch (HaltException he) {
        assertEquals("ExitUtil.halt should have rethrown its HaltException argument but it had "
            +"thrown something else", he1, he);
      }
      assertTrue("ExitUtil.halt/haltCalled should remember halt has been called",
          ExitUtil.haltCalled());
      assertEquals("ExitUtil.halt should store its 1st call's HaltException",
          he1, ExitUtil.getFirstHaltException());

      // simulate/check 2nd call not overwritting 1st one
      try {
        ExitUtil.halt(he2);
        fail("ExitUtil.halt should have rethrown its HaltException argument but it returned");
      } catch (HaltException he) {
        assertEquals("ExitUtil.halt should have rethrown its HaltException argument but it had "
            +"thrown something else", he2, he);
      }
      assertTrue(ExitUtil.haltCalled());
      assertEquals("ExitUtil.halt when called twice should only remember 1st call's HaltException",
          he1, ExitUtil.getFirstHaltException());

      // simulate cleanup, also tries to make sure state is ok for all junit still has to do
      ExitUtil.resetFirstHaltException();
      assertFalse(ExitUtil.haltCalled());
      assertNull(ExitUtil.getFirstHaltException());
    } finally {
      // cleanup, useless if last test succeed, useful if not
      ExitUtil.resetFirstHaltException();
    }
  }
}
