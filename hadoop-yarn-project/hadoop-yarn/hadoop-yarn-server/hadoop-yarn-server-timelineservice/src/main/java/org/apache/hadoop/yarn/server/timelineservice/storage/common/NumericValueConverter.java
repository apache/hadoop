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

package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import java.util.Comparator;

/**
 * Extends ValueConverter interface for numeric converters to support numerical
 * operations such as comparison, addition, etc.
 */
public interface NumericValueConverter extends ValueConverter,
    Comparator<Number> {
  /**
   * Adds two or more numbers. If either of the numbers are null, it is taken as
   * 0.
   *
   * @param num1 the first number to add.
   * @param num2 the second number to add.
   * @param numbers Rest of the numbers to be added.
   * @return result after adding up the numbers.
   */
  Number add(Number num1, Number num2, Number...numbers);
}
