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

package org.apache.hadoop.security.token;

import java.io.*;
import java.util.Arrays;

import org.apache.hadoop.io.*;

import junit.framework.TestCase;

/** Unit tests for Token */
public class TestToken extends TestCase {

  static boolean isEqual(Object a, Object b) {
    return a == null ? b == null : a.equals(b);
  }

  static boolean checkEqual(Token<TokenIdentifier> a, Token<TokenIdentifier> b) {
    return Arrays.equals(a.getIdentifier(), b.getIdentifier())
        && Arrays.equals(a.getPassword(), b.getPassword())
        && isEqual(a.getKind(), b.getKind())
        && isEqual(a.getService(), b.getService());
  }

  /**
   * Test token serialization
   */
  public void testTokenSerialization() throws IOException {
    // Get a token
    Token<TokenIdentifier> sourceToken = new Token<TokenIdentifier>();
    sourceToken.setService(new Text("service"));

    // Write it to an output buffer
    DataOutputBuffer out = new DataOutputBuffer();
    sourceToken.write(out);

    // Read the token back
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    Token<TokenIdentifier> destToken = new Token<TokenIdentifier>();
    destToken.readFields(in);
    assertTrue(checkEqual(sourceToken, destToken));
  }
}
