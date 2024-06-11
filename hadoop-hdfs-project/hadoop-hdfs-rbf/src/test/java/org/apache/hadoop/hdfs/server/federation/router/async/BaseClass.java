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
package org.apache.hadoop.hdfs.server.federation.router.async;

import java.io.IOException;
import java.util.List;

/**
 * It defines a set of methods that can be executed either synchronously
 * or asynchronously, depending on the implementation.
 *
 * <p>
 * This interface is designed to abstract the common operations that need
 * to be performed in a time-consuming manner, such as processing a list
 * of items or applying a method that involves I/O operations. By defining
 * these methods in an interface, it allows for both synchronous and
 * asynchronous implementations, providing flexibility and the ability to
 * improve performance without changing the external API.
 * </p>
 *
 * <p>
 * Implementations of this interface are expected to provide concrete
 * implementations of the defined methods, either by performing the
 * operations synchronously in a blocking manner or by performing them
 * asynchronously in a non-blocking manner.
 * </p>
 *
 * @see SyncClass
 * @see AsyncClass
 */
public interface BaseClass {
  String applyMethod(int input);

  String applyMethod(int input, boolean canException) throws IOException;

  String exceptionMethod(int input) throws IOException;

  String forEachMethod(List<Integer> list);

  String forEachBreakMethod(List<Integer> list);

  String forEachBreakByExceptionMethod(List<Integer> list);

  String applyThenApplyMethod(int input);

  String applyCatchThenApplyMethod(int input);

  String applyCatchFinallyMethod(int input, List<String> resource) throws IOException;

  String currentMethod(List<Integer> list);
}
