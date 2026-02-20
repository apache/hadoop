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

import static org.apache.hadoop.fs.shell.find.TestHelper.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.fs.shell.PathData;

public class TestNumberExpression {

  private final NumberExpression numberExpr = new NumberExpression() {
    @Override
    public Result apply(PathData item, int depth) {
      return null;
    }
  };

  // test the applyNumber method with an exact match
  @Test
  public void applyNumberEquals() throws IOException {
    addArgument(numberExpr, "5");
    numberExpr.setOptions(new FindOptions());
    numberExpr.prepare();
    assertEquals(Result.PASS, numberExpr.applyNumber(5));
    assertEquals(Result.FAIL, numberExpr.applyNumber(4));
    assertEquals(Result.FAIL, numberExpr.applyNumber(6));
  }

  // test the applyNumber method with a greater than match
  @Test
  public void applyNumberGreaterThan() throws IOException {
    addArgument(numberExpr, "+5");
    numberExpr.setOptions(new FindOptions());
    numberExpr.prepare();
    assertEquals(Result.FAIL, numberExpr.applyNumber(5));
    assertEquals(Result.FAIL, numberExpr.applyNumber(4));
    assertEquals(Result.PASS, numberExpr.applyNumber(6));
  }

  // test the applyNumber method with a less than match
  @Test
  public void applyNumberLessThan() throws IOException {
    addArgument(numberExpr, "-5");
    numberExpr.setOptions(new FindOptions());
    numberExpr.prepare();
    assertEquals(Result.FAIL, numberExpr.applyNumber(5));
    assertEquals(Result.PASS, numberExpr.applyNumber(4));
    assertEquals(Result.FAIL, numberExpr.applyNumber(6));
  }

  // test an invalid empty argument throws an exception
  @Test
  public void testInvalidEmptyArgument() throws IOException {
    addArgument(numberExpr, "");
    numberExpr.setOptions(new FindOptions());
    IllegalArgumentException expected =
        assertThrows(IllegalArgumentException.class, numberExpr::prepare);
    assertEquals("Invalid empty argument", expected.getMessage());
  }
}
