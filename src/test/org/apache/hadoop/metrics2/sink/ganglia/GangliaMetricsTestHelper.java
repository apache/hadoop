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

package org.apache.hadoop.metrics2.sink.ganglia;

import java.net.DatagramSocket;

/**
 * Helper class in the same package as ganglia sinks to be used by unit tests
 */
public class GangliaMetricsTestHelper {

  /**
   * Helper method to access package private method to set DatagramSocket
   * needed for Unit test
   * @param gangliaSink
   * @param datagramSocket
   */
  public static void setDatagramSocket(AbstractGangliaSink gangliaSink,
      DatagramSocket datagramSocket) {

    gangliaSink.setDatagramSocket(datagramSocket);
  }
}
