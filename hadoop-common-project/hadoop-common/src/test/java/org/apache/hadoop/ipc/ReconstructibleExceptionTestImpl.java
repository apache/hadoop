/*
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

package org.apache.hadoop.ipc;

import java.io.IOException;

/**
 * Test class of ReconstructibleException.
 */
public class ReconstructibleExceptionTestImpl extends IOException
    implements ReconstructibleException<ReconstructibleExceptionTestImpl> {

  private int field1;
  private String field2;

  public ReconstructibleExceptionTestImpl(String msg) {
    super(msg);
  }

  public ReconstructibleExceptionTestImpl(String param1, String param2) {
    super(param1 + param2);
    field1 = Integer.parseInt(param1);
    field2 = param2;
  }

  @Override
  public ReconstructibleExceptionTestImpl reconstruct(String... params) {
    return new ReconstructibleExceptionTestImpl(params[0], params[1]);
  }

  @Override
  public String[] getReconstructParams() {
    return new String[]{String.valueOf(field1), field2};
  }

  public int getField1() {
    return field1;
  }

  public String getField2() {
    return field2;
  }
}
