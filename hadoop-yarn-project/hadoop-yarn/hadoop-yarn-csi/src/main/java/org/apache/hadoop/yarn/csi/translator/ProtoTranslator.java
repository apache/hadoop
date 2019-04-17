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
package org.apache.hadoop.yarn.csi.translator;

import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * ProtoTranslator converts a YARN side message to CSI proto message
 * and vice versa. Each CSI proto message should have a corresponding
 * YARN side message implementation, and a transformer to convert them
 * one to the other. This layer helps we to hide CSI spec messages
 * from YARN components.
 *
 * @param <A> YARN side internal messages
 * @param <B> CSI proto messages
 */
public interface ProtoTranslator<A, B> {

  /**
   * Convert message from type A to type B.
   * @param messageA
   * @return messageB
   * @throws YarnException
   */
  B convertTo(A messageA) throws YarnException;

  /**
   * Convert message from type B to type A.
   * @param messageB
   * @return messageA
   * @throws YarnException
   */
  A convertFrom(B messageB) throws YarnException;
}
