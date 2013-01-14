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
package org.apache.hadoop.tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;

public class FakeRenewer extends TokenRenewer {
  static Token<?> lastRenewed = null;
  static Token<?> lastCanceled = null;
  static final Text KIND = new Text("TESTING-TOKEN-KIND");

  @Override
  public boolean handleKind(Text kind) {
    return FakeRenewer.KIND.equals(kind);
  }

  @Override
  public boolean isManaged(Token<?> token) throws IOException {
    return true;
  }

  @Override
  public long renew(Token<?> token, Configuration conf) {
    lastRenewed = token;
    return 0;
  }

  @Override
  public void cancel(Token<?> token, Configuration conf) {
    lastCanceled = token;
  }

  public static void reset() {
    lastRenewed = null;
    lastCanceled = null;
  }
}
