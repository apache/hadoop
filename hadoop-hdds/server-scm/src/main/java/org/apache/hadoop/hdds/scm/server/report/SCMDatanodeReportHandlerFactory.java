/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.server.report;

import com.google.protobuf.GeneratedMessage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.util.ReflectionUtils;

import java.util.HashMap;
import java.util.Map;


/**
 * Factory class to construct {@link SCMDatanodeReportHandler} given a report.
 */
public class SCMDatanodeReportHandlerFactory {

  private final Configuration conf;
  private final StorageContainerManager scm;
  private final Map<Class<? extends GeneratedMessage>,
      Class<? extends SCMDatanodeReportHandler<? extends GeneratedMessage>>>
      report2handler;

  /**
   * Constructs {@link SCMDatanodeReportHandler} instance.
   *
   * @param conf Configuration to be passed to the
   *               {@link SCMDatanodeReportHandler}
   */
  public SCMDatanodeReportHandlerFactory(Configuration conf,
                                         StorageContainerManager scm) {
    this.conf = conf;
    this.scm = scm;
    this.report2handler = new HashMap<>();

    report2handler.put(NodeReportProto.class,
        SCMDatanodeNodeReportHandler.class);
    report2handler.put(ContainerReportsProto.class,
        SCMDatanodeContainerReportHandler.class);
  }

  /**
   * Returns the SCMDatanodeReportHandler for the corresponding report.
   *
   * @param report report
   *
   * @return report handler
   */
  public SCMDatanodeReportHandler<? extends GeneratedMessage> getHandlerFor(
      Class<? extends GeneratedMessage> report) {
    Class<? extends SCMDatanodeReportHandler<? extends GeneratedMessage>>
        handlerClass = report2handler.get(report);
    if (handlerClass == null) {
      throw new RuntimeException("No handler found for report " + report);
    }
    SCMDatanodeReportHandler<? extends GeneratedMessage> instance =
    ReflectionUtils.newInstance(handlerClass, conf);
    instance.init(scm);
    return instance;
  }

}