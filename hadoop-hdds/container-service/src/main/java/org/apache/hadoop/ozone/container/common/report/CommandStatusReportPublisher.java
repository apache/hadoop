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

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.CommandStatus.Status;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.hdds.scm.HddsServerUtil;
import org.apache.hadoop.ozone.protocol.commands.CommandStatus;

import static org.apache.hadoop.hdds.HddsConfigKeys
    .HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys
    .HDDS_COMMAND_STATUS_REPORT_INTERVAL_DEFAULT;

/**
 * Publishes CommandStatusReport which will be sent to SCM as part of
 * heartbeat. CommandStatusReport consist of the following information:
 * - type       : type of command.
 * - status     : status of command execution (PENDING, EXECUTED, FAILURE).
 * - cmdId      : Command id.
 * - msg        : optional message.
 */
public class CommandStatusReportPublisher extends
    ReportPublisher<CommandStatusReportsProto> {

  private long cmdStatusReportInterval = -1;

  @Override
  protected long getReportFrequency() {
    if (cmdStatusReportInterval == -1) {
      cmdStatusReportInterval = getConf().getTimeDuration(
          HDDS_COMMAND_STATUS_REPORT_INTERVAL,
          HDDS_COMMAND_STATUS_REPORT_INTERVAL_DEFAULT,
          TimeUnit.MILLISECONDS);

      long heartbeatFrequency = HddsServerUtil.getScmHeartbeatInterval(
          getConf());

      Preconditions.checkState(
          heartbeatFrequency <= cmdStatusReportInterval,
          HDDS_COMMAND_STATUS_REPORT_INTERVAL +
              " cannot be configured lower than heartbeat frequency.");
    }
    return cmdStatusReportInterval;
  }

  @Override
  protected CommandStatusReportsProto getReport() {
    Map<Long, CommandStatus> map = this.getContext()
        .getCommandStatusMap();
    Iterator<Long> iterator = map.keySet().iterator();
    CommandStatusReportsProto.Builder builder = CommandStatusReportsProto
        .newBuilder();

    iterator.forEachRemaining(key -> {
      CommandStatus cmdStatus = map.get(key);
      // If status is still pending then don't remove it from map as
      // CommandHandler will change its status when it works on this command.
      if (!cmdStatus.getStatus().equals(Status.PENDING)) {
        builder.addCmdStatus(cmdStatus.getProtoBufMessage());
        map.remove(key);
      }
    });
    return builder.getCmdStatusCount() > 0 ? builder.build() : null;
  }
}
