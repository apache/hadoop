/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.swift.scale;

import org.apache.hadoop.fs.swift.SwiftFileSystemBaseTest;

/**
 * Base class for scale tests; here is where the common scale configuration
 * keys are defined
 */

public class SwiftScaleTestBase extends SwiftFileSystemBaseTest {

  public static final String SCALE_TEST = "scale.test.";
  public static final String KEY_OPERATION_COUNT = SCALE_TEST + "operation.count";
  public static final long DEFAULT_OPERATION_COUNT = 10;

  protected long getOperationCount() {
    return getConf().getLong(KEY_OPERATION_COUNT, DEFAULT_OPERATION_COUNT);
  }
}
