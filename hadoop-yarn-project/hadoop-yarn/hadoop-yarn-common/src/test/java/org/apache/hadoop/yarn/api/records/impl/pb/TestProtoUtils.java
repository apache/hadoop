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
package org.apache.hadoop.yarn.api.records.impl.pb;

import static org.junit.Assert.*;

import java.util.stream.Stream;

import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerSubState;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerStateProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerSubStateProto;
import org.junit.Test;

public class TestProtoUtils {

  @Test
  public void testConvertFromOrToProtoFormat() {
    // Check if utility has all enum values
    try {
      Stream.of(ContainerState.values())
          .forEach(a -> ProtoUtils.convertToProtoFormat(a));
      Stream.of(ContainerSubState.values())
          .forEach(a -> ProtoUtils.convertToProtoFormat(a));
      Stream.of(ContainerSubStateProto.values())
          .forEach(a -> ProtoUtils.convertFromProtoFormat(a));
      Stream.of(ContainerStateProto.values())
          .forEach(a -> ProtoUtils.convertFromProtoFormat(a));
    } catch (IllegalArgumentException ex) {
      fail(ex.getMessage());
    }
  }
}
