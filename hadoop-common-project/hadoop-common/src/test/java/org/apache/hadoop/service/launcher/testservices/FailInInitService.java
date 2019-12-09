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

package org.apache.hadoop.service.launcher.testservices;

/**
 * Service which fails in its init() operation.
 */
public class FailInInitService extends FailureTestService {
  public static final String NAME =
      "org.apache.hadoop.service.launcher.testservices.FailInInitService";
  public static final int EXIT_CODE = -1;

  public FailInInitService() {
    super(true, false, false, 0
    );
  }

  @Override
  int getExitCode() {
    return EXIT_CODE;
  }
}
