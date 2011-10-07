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
package org.apache.hadoop.log;

/**
 * A log4J Appender that simply counts logging events in three levels:
 * fatal, error and warn. The class name is used in log4j.properties
 * @deprecated use {@link org.apache.hadoop.log.metrics.EventCounter} instead
 */
@Deprecated
public class EventCounter extends org.apache.hadoop.log.metrics.EventCounter {
  static {
    // The logging system is not started yet.
    System.err.println("WARNING: "+ EventCounter.class.getName() +
        " is deprecated. Please use "+
        org.apache.hadoop.log.metrics.EventCounter.class.getName() +
        " in all the log4j.properties files.");
  }
}
