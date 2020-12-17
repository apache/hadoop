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

package org.apache.hadoop.util.functional;

import java.io.IOException;

/**
 * This is a lambda-expression which may raises an IOException.
 * This is a recurrent design patten in the hadoop codebase, e.g
 * {@code LambdaTestUtils.VoidCallable} and
 * the S3A {@code Invoker.VoidOperation}}. Hopefully this should
 * be the last.
 * Note for implementors of methods which take this as an argument:
 * don't use method overloading to determine which specific functional
 * interface is to be used.
 */
@FunctionalInterface
public interface InvocationRaisingIOE {

  /**
   * Apply the operation.
   * @throws IOException Any IO failure
   */
  void apply() throws IOException;

}
