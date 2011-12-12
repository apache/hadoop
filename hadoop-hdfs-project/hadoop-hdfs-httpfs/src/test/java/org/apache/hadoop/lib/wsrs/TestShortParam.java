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

package org.apache.hadoop.lib.wsrs;


import junit.framework.Assert;
import org.junit.Test;

public class TestShortParam {

  @Test
  public void param() throws Exception {
    ShortParam param = new ShortParam("p", "1") {
    };
    Assert.assertEquals(param.getDomain(), "a short");
    Assert.assertEquals(param.value(), new Short((short) 1));
    Assert.assertEquals(param.toString(), "1");
    param = new ShortParam("p", null) {
    };
    Assert.assertEquals(param.value(), null);
    param = new ShortParam("p", "") {
    };
    Assert.assertEquals(param.value(), null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalid1() throws Exception {
    new ShortParam("p", "x") {
    };
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalid2() throws Exception {
    new ShortParam("p", "" + Integer.MAX_VALUE) {
    };
  }
}
