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

package org.apache.hadoop.ipc;

import java.io.IOException;

import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.specific.SpecificRequestor;
import org.apache.avro.specific.SpecificResponder;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * AvroRpcEngine which uses Avro's "specific" APIs. The protocols generated 
 * via Avro IDL needs to use this Engine.
 */
@InterfaceStability.Evolving
public class AvroSpecificRpcEngine extends AvroRpcEngine {

  protected SpecificRequestor createRequestor(Class<?> protocol, 
      Transceiver transeiver) throws IOException {
    return new SpecificRequestor(protocol, transeiver);
  }

  protected Responder createResponder(Class<?> iface, Object impl) {
    return new SpecificResponder(iface, impl);
  }

}
