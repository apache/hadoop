/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
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

package org.apache.hadoop.ozone.container.common.report;

import com.google.protobuf.GeneratedMessage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.proto.
        StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.util.ReflectionUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Factory class to construct {@link ReportPublisher} for a report.
 */
public class ReportPublisherFactory {

  private final Configuration conf;
  private final Map<Class<? extends GeneratedMessage>,
      Class<? extends ReportPublisher>> report2publisher;

  /**
   * Constructs {@link ReportPublisherFactory} instance.
   *
   * @param conf Configuration to be passed to the {@link ReportPublisher}
   */
  public ReportPublisherFactory(Configuration conf) {
    this.conf = conf;
    this.report2publisher = new HashMap<>();

    report2publisher.put(NodeReportProto.class, NodeReportPublisher.class);
    report2publisher.put(ContainerReportsProto.class,
        ContainerReportPublisher.class);
    report2publisher.put(CommandStatusReportsProto.class,
        CommandStatusReportPublisher.class);
    report2publisher.put(PipelineReportsProto.class,
            PipelineReportPublisher.class);
  }

  /**
   * Returns the ReportPublisher for the corresponding report.
   *
   * @param report report
   *
   * @return report publisher
   */
  public ReportPublisher getPublisherFor(
      Class<? extends GeneratedMessage> report) {
    Class<? extends ReportPublisher> publisherClass =
        report2publisher.get(report);
    if (publisherClass == null) {
      throw new RuntimeException("No publisher found for report " + report);
    }
    return ReflectionUtils.newInstance(publisherClass, conf);
  }

}
