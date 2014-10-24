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

package org.apache.hadoop.yarn.api;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateResponsePBImpl;
import org.apache.hadoop.yarn.api.records.AMCommand;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerResourceDecrease;
import org.apache.hadoop.yarn.api.records.ContainerResourceIncrease;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AllocateResponseProto;
import org.junit.Assert;
import org.junit.Test;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
public class TestAllocateResponse {
  @SuppressWarnings("deprecation")
  @Test
  public void testAllocateResponseWithIncDecContainers() {
    List<ContainerResourceIncrease> incContainers =
        new ArrayList<ContainerResourceIncrease>();
    List<ContainerResourceDecrease> decContainers =
        new ArrayList<ContainerResourceDecrease>();
    for (int i = 0; i < 3; i++) {
      incContainers.add(ContainerResourceIncrease.newInstance(null,
          Resource.newInstance(1024, i), null));
    }
    for (int i = 0; i < 5; i++) {
      decContainers.add(ContainerResourceDecrease.newInstance(null,
          Resource.newInstance(1024, i)));
    }

    AllocateResponse r =
        AllocateResponse.newInstance(3, new ArrayList<ContainerStatus>(),
            new ArrayList<Container>(), new ArrayList<NodeReport>(), null,
            AMCommand.AM_RESYNC, 3, null, new ArrayList<NMToken>(),
            incContainers, decContainers);

    // serde
    AllocateResponseProto p = ((AllocateResponsePBImpl) r).getProto();
    r = new AllocateResponsePBImpl(p);

    // check value
    Assert
        .assertEquals(incContainers.size(), r.getIncreasedContainers().size());
    Assert
        .assertEquals(decContainers.size(), r.getDecreasedContainers().size());

    for (int i = 0; i < incContainers.size(); i++) {
      Assert.assertEquals(i, r.getIncreasedContainers().get(i).getCapability()
          .getVirtualCores());
    }

    for (int i = 0; i < decContainers.size(); i++) {
      Assert.assertEquals(i, r.getDecreasedContainers().get(i).getCapability()
          .getVirtualCores());
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testAllocateResponseWithoutIncDecContainers() {
    AllocateResponse r =
        AllocateResponse.newInstance(3, new ArrayList<ContainerStatus>(),
            new ArrayList<Container>(), new ArrayList<NodeReport>(), null,
            AMCommand.AM_RESYNC, 3, null, new ArrayList<NMToken>(), null, null);

    // serde
    AllocateResponseProto p = ((AllocateResponsePBImpl) r).getProto();
    r = new AllocateResponsePBImpl(p);

    // check value
    Assert.assertEquals(0, r.getIncreasedContainers().size());
    Assert.assertEquals(0, r.getDecreasedContainers().size());
  }
}
