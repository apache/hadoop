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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;

import org.junit.Test;

public class TestInputStreamEntity {

  @Test
  public void test() throws Exception {
    InputStream is = new ByteArrayInputStream("abc".getBytes());
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    InputStreamEntity i = new InputStreamEntity(is);
    i.write(baos);
    baos.close();
    assertEquals(new String(baos.toByteArray()), "abc");

    is = new ByteArrayInputStream("abc".getBytes());
    baos = new ByteArrayOutputStream();
    i = new InputStreamEntity(is, 1, 1);
    i.write(baos);
    baos.close();
    assertEquals(baos.toByteArray()[0], 'b');
  }

}
