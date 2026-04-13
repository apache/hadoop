/*
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

package org.apache.hadoop.mapred.protocolPB;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.AMFeedback;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JvmContext;
import org.apache.hadoop.mapred.JvmTask;
import org.apache.hadoop.mapred.MapTaskCompletionEventsUpdate;
import org.apache.hadoop.mapred.SortedRanges;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.CanCommitRequestProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.CommitPendingRequestProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.DoneRequestProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.FatalErrorRequestProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.FsErrorRequestProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.GetCheckpointIDRequestProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.GetCheckpointIDResponseProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.GetMapCompletionEventsRequestProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.GetMapCompletionEventsResponseProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.GetTaskRequestProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.GetTaskResponseProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.PreemptedRequestProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.ReportDiagnosticInfoRequestProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.ReportNextRecordRangeRequestProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.SetCheckpointIDRequestProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.ShuffleErrorRequestProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.StatusUpdateRequestProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.StatusUpdateResponseProto;
import org.apache.hadoop.mapreduce.checkpoint.TaskCheckpointID;

import static org.apache.hadoop.ipc.RPC.stopProxy;
import static org.apache.hadoop.mapred.protocolPB.TaskUmbilicalProtocolUtils.service;

/**
 * Client-side translator to translate calls from {@link TaskUmbilicalProtocol}
 * to the RPC server implementing {@link TaskUmbilicalProtocolPB}.
 */
@InterfaceAudience.Private
public class TaskUmbilicalProtocolPBClientImpl
    implements TaskUmbilicalProtocol, Closeable {

  private final TaskUmbilicalProtocolPB proxy;

  public TaskUmbilicalProtocolPBClientImpl(TaskUmbilicalProtocolPB proxy) {
    this.proxy = proxy;
  }

  @Override
  public void close() throws IOException {
    stopProxy(proxy);
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return RPC.getProtocolVersion(TaskUmbilicalProtocolPB.class);
  }

  @Override
  public ProtocolSignature getProtocolSignature(
      String protocol, long clientVersion, int clientMethodsHash)
      throws IOException {
    return ProtocolSignature.getProtocolSignature(this,
        protocol, clientVersion, clientMethodsHash);
  }

  @Override
  public JvmTask getTask(JvmContext context) throws IOException {
    return service(() -> {
      GetTaskRequestProto.Builder builder = GetTaskRequestProto.newBuilder();
      if (context != null) {
        builder.setJvmContext(
            TaskUmbilicalProtocolUtils.serialize(context));
      }
      GetTaskResponseProto response =
          proxy.getTask(null, builder.build());
      if (!response.hasJvmTask()) {
        return null;
      }
      return TaskUmbilicalProtocolUtils.deserialize(
          new JvmTask(), response.getJvmTask());
    });
  }

  @Override
  public AMFeedback statusUpdate(TaskAttemptID taskId, TaskStatus taskStatus)
      throws IOException, InterruptedException {
    return service(() -> {
      StatusUpdateRequestProto.Builder builder =
          StatusUpdateRequestProto.newBuilder();
      if (taskId != null) {
        builder.setTaskId(
            TaskUmbilicalProtocolUtils.serialize(taskId));
      }
      if (taskStatus != null) {
        builder.setTaskStatus(
            TaskUmbilicalProtocolUtils.serializeTaskStatus(
                taskStatus));
      }
      StatusUpdateResponseProto response =
          proxy.statusUpdate(null, builder.build());
      AMFeedback feedback = new AMFeedback();
      feedback.setTaskFound(response.getTaskFound());
      feedback.setPreemption(response.getPreemption());
      return feedback;
    });
  }

  @Override
  public void reportDiagnosticInfo(TaskAttemptID taskid, String trace)
      throws IOException {
    service(() -> {
      ReportDiagnosticInfoRequestProto.Builder builder =
          ReportDiagnosticInfoRequestProto.newBuilder();
      if (taskid != null) {
        builder.setTaskId(
            TaskUmbilicalProtocolUtils.serialize(taskid));
      }
      builder.setTrace(trace != null ? trace : "");
      proxy.reportDiagnosticInfo(null, builder.build());
    });
  }

  @Override
  public void reportNextRecordRange(TaskAttemptID taskid,
      SortedRanges.Range range) throws IOException {
    service(() -> {
      ReportNextRecordRangeRequestProto.Builder builder =
          ReportNextRecordRangeRequestProto.newBuilder();
      if (taskid != null) {
        builder.setTaskId(
            TaskUmbilicalProtocolUtils.serialize(taskid));
      }
      if (range != null) {
        builder.setRange(
            TaskUmbilicalProtocolUtils.serialize(range));
      }
      proxy.reportNextRecordRange(null, builder.build());
    });
  }

  @Override
  public void done(TaskAttemptID taskid) throws IOException {
    service(() -> {
      DoneRequestProto.Builder builder = DoneRequestProto.newBuilder();
      if (taskid != null) {
        builder.setTaskId(
            TaskUmbilicalProtocolUtils.serialize(taskid));
      }
      proxy.done(null, builder.build());
    });
  }

  @Override
  public void commitPending(TaskAttemptID taskId, TaskStatus taskStatus)
      throws IOException, InterruptedException {
    service(() -> {
      CommitPendingRequestProto.Builder builder =
          CommitPendingRequestProto.newBuilder();
      if (taskId != null) {
        builder.setTaskId(
            TaskUmbilicalProtocolUtils.serialize(taskId));
      }
      if (taskStatus != null) {
        builder.setTaskStatus(
            TaskUmbilicalProtocolUtils.serializeTaskStatus(
                taskStatus));
      }
      proxy.commitPending(null, builder.build());
    });
  }

  @Override
  public boolean canCommit(TaskAttemptID taskid) throws IOException {
    return service(() -> {
      CanCommitRequestProto.Builder builder =
          CanCommitRequestProto.newBuilder();
      if (taskid != null) {
        builder.setTaskId(
            TaskUmbilicalProtocolUtils.serialize(taskid));
      }
      return proxy.canCommit(null, builder.build()).getCanCommit();
    });
  }

  @Override
  public void shuffleError(TaskAttemptID taskId, String message)
      throws IOException {
    service(() -> {
      ShuffleErrorRequestProto.Builder builder =
          ShuffleErrorRequestProto.newBuilder();
      if (taskId != null) {
        builder.setTaskId(
            TaskUmbilicalProtocolUtils.serialize(taskId));
      }
      builder.setMessage(message != null ? message : "");
      proxy.shuffleError(null, builder.build());
    });
  }

  @Override
  public void fsError(TaskAttemptID taskId, String message) throws IOException {
    service(() -> {
      FsErrorRequestProto.Builder builder = FsErrorRequestProto.newBuilder();
      if (taskId != null) {
        builder.setTaskId(
            TaskUmbilicalProtocolUtils.serialize(taskId));
      }
      builder.setMessage(message != null ? message : "");
      proxy.fsError(null, builder.build());
    });
  }

  @Override
  public void fatalError(TaskAttemptID taskId, String msg, boolean fastFail)
      throws IOException {
    service(() -> {
      FatalErrorRequestProto.Builder builder =
          FatalErrorRequestProto.newBuilder();
      if (taskId != null) {
        builder.setTaskId(
            TaskUmbilicalProtocolUtils.serialize(taskId));
      }
      builder.setMessage(msg != null ? msg : "");
      builder.setFastFail(fastFail);
      proxy.fatalError(null, builder.build());
    });
  }

  @Override
  public MapTaskCompletionEventsUpdate getMapCompletionEvents(JobID jobId,
      int fromIndex, int maxLocs, TaskAttemptID id) throws IOException {
    return service(() -> {
      GetMapCompletionEventsRequestProto.Builder builder =
          GetMapCompletionEventsRequestProto.newBuilder();
      if (jobId != null) {
        builder.setJobId(
            TaskUmbilicalProtocolUtils.serialize(jobId));
      }
      builder.setFromIndex(fromIndex);
      builder.setMaxLocs(maxLocs);
      if (id != null) {
        builder.setTaskAttemptId(
            TaskUmbilicalProtocolUtils.serialize(id));
      }
      GetMapCompletionEventsResponseProto response =
          proxy.getMapCompletionEvents(null, builder.build());
      if (!response.hasEventsUpdate()) {
        return null;
      }
      return TaskUmbilicalProtocolUtils.deserialize(
          new MapTaskCompletionEventsUpdate(), response.getEventsUpdate());
    });
  }

  @Override
  public void preempted(TaskAttemptID taskId, TaskStatus taskStatus)
      throws IOException, InterruptedException {
    service(() -> {
      PreemptedRequestProto.Builder builder = PreemptedRequestProto.newBuilder();
      if (taskId != null) {
        builder.setTaskId(
            TaskUmbilicalProtocolUtils.serialize(taskId));
      }
      if (taskStatus != null) {
        builder.setTaskStatus(
            TaskUmbilicalProtocolUtils.serializeTaskStatus(
                taskStatus));
      }
      proxy.preempted(null, builder.build());
    });
  }

  @Override
  public TaskCheckpointID getCheckpointID(TaskID taskId) {
    return service(() -> {
      GetCheckpointIDRequestProto.Builder builder =
          GetCheckpointIDRequestProto.newBuilder();
      if (taskId != null) {
        builder.setTaskId(
            TaskUmbilicalProtocolUtils.serialize(taskId));
      }
      GetCheckpointIDResponseProto response =
          proxy.getCheckpointID(null, builder.build());
      if (!response.hasCheckpointId()) {
        return null;
      }
      return TaskUmbilicalProtocolUtils.deserialize(
          new TaskCheckpointID(), response.getCheckpointId());
    });
  }

  @Override
  public void setCheckpointID(TaskID taskId, TaskCheckpointID checkpointId) {
    service(() -> {
      SetCheckpointIDRequestProto.Builder builder =
          SetCheckpointIDRequestProto.newBuilder();
      if (taskId != null) {
        builder.setTaskId(
            TaskUmbilicalProtocolUtils.serialize(taskId));
      }
      if (checkpointId != null) {
        builder.setCheckpointId(
            TaskUmbilicalProtocolUtils.serialize(checkpointId));
      }
      proxy.setCheckpointID(null, builder.build());
    });
  }


}
