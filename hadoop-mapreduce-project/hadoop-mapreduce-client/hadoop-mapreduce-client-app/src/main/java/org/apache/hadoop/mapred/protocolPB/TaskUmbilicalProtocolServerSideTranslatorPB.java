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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
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
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.CanCommitResponseProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.CommitPendingRequestProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.CommitPendingResponseProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.DoneRequestProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.DoneResponseProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.FatalErrorRequestProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.FatalErrorResponseProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.FsErrorRequestProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.FsErrorResponseProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.GetCheckpointIDRequestProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.GetCheckpointIDResponseProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.GetMapCompletionEventsRequestProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.GetMapCompletionEventsResponseProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.GetTaskRequestProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.GetTaskResponseProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.PreemptedRequestProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.PreemptedResponseProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.ReportDiagnosticInfoRequestProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.ReportDiagnosticInfoResponseProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.ReportNextRecordRangeRequestProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.ReportNextRecordRangeResponseProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.SetCheckpointIDRequestProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.SetCheckpointIDResponseProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.ShuffleErrorRequestProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.ShuffleErrorResponseProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.StatusUpdateRequestProto;
import org.apache.hadoop.mapred.proto.TaskUmbilicalProtocolProtos.StatusUpdateResponseProto;
import org.apache.hadoop.mapreduce.checkpoint.TaskCheckpointID;
import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

/**
 * Server-side translator to translate the requests received on
 * {@link TaskUmbilicalProtocolPB} to the calls in {@link TaskUmbilicalProtocol}.
 */
@InterfaceAudience.Private
public class TaskUmbilicalProtocolServerSideTranslatorPB
    implements TaskUmbilicalProtocolPB {
  private static final ReportDiagnosticInfoResponseProto VOID_REPORT_DIAG_RESPONSE =
      ReportDiagnosticInfoResponseProto.newBuilder().build();

  private static final ReportNextRecordRangeResponseProto VOID_REPORT_RANGE_RESPONSE =
      ReportNextRecordRangeResponseProto.newBuilder().build();

  private static final DoneResponseProto VOID_DONE_RESPONSE =
      DoneResponseProto.newBuilder().build();

  private static final CommitPendingResponseProto VOID_COMMIT_PENDING_RESPONSE =
      CommitPendingResponseProto.newBuilder().build();

  private static final ShuffleErrorResponseProto VOID_SHUFFLE_ERROR_RESPONSE =
      ShuffleErrorResponseProto.newBuilder().build();

  private static final FsErrorResponseProto VOID_FS_ERROR_RESPONSE =
      FsErrorResponseProto.newBuilder().build();

  private static final FatalErrorResponseProto VOID_FATAL_ERROR_RESPONSE =
      FatalErrorResponseProto.newBuilder().build();

  private static final PreemptedResponseProto VOID_PREEMPTED_RESPONSE =
      PreemptedResponseProto.newBuilder().build();

  private static final SetCheckpointIDResponseProto VOID_SET_CHECKPOINT_RESPONSE =
      SetCheckpointIDResponseProto.newBuilder().build();


  private final TaskUmbilicalProtocol impl;

  public TaskUmbilicalProtocolServerSideTranslatorPB(TaskUmbilicalProtocol impl) {
    this.impl = impl;
  }

  @Override
  public GetTaskResponseProto getTask(RpcController controller,
      GetTaskRequestProto request) throws ServiceException {
    try {
      JvmContext context = request.hasJvmContext()
          ? deserialize(new JvmContext(), request.getJvmContext())
          : null;
      JvmTask task = impl.getTask(context);
      GetTaskResponseProto.Builder builder = GetTaskResponseProto.newBuilder();
      if (task != null) {
        builder.setJvmTask(serialize(task));
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public StatusUpdateResponseProto statusUpdate(RpcController controller,
      StatusUpdateRequestProto request) throws ServiceException {
    try {
      TaskAttemptID taskId = request.hasTaskId()
          ? deserialize(new TaskAttemptID(), request.getTaskId())
          : null;
      TaskStatus taskStatus = null;
      if (request.hasTaskStatus()) {
        taskStatus = deserializeTaskStatus(request.getTaskStatus());
      }
      AMFeedback feedback = impl.statusUpdate(taskId, taskStatus);
      return StatusUpdateResponseProto.newBuilder()
          .setTaskFound(feedback.getTaskFound())
          .setPreemption(feedback.getPreemption())
          .build();
    } catch (IOException | InterruptedException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ReportDiagnosticInfoResponseProto reportDiagnosticInfo(
      RpcController controller,
      ReportDiagnosticInfoRequestProto request) throws ServiceException {
    try {
      TaskAttemptID taskId = request.hasTaskId()
          ? deserialize(new TaskAttemptID(), request.getTaskId())
          : null;
      impl.reportDiagnosticInfo(taskId, request.getTrace());
      return VOID_REPORT_DIAG_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ReportNextRecordRangeResponseProto reportNextRecordRange(
      RpcController controller,
      ReportNextRecordRangeRequestProto request) throws ServiceException {
    try {
      TaskAttemptID taskId = request.hasTaskId()
          ? deserialize(new TaskAttemptID(), request.getTaskId())
          : null;
      SortedRanges.Range range = request.hasRange()
          ? deserialize(new SortedRanges.Range(), request.getRange())
          : null;
      impl.reportNextRecordRange(taskId, range);
      return VOID_REPORT_RANGE_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public DoneResponseProto done(RpcController controller,
      DoneRequestProto request) throws ServiceException {
    try {
      TaskAttemptID taskId = request.hasTaskId()
          ? deserialize(new TaskAttemptID(), request.getTaskId())
          : null;
      impl.done(taskId);
      return VOID_DONE_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public CommitPendingResponseProto commitPending(RpcController controller,
      CommitPendingRequestProto request) throws ServiceException {
    try {
      TaskAttemptID taskId = request.hasTaskId()
          ? deserialize(new TaskAttemptID(), request.getTaskId())
          : null;
      TaskStatus taskStatus = null;
      if (request.hasTaskStatus()) {
        taskStatus = deserializeTaskStatus(request.getTaskStatus());
      }
      impl.commitPending(taskId, taskStatus);
      return VOID_COMMIT_PENDING_RESPONSE;
    } catch (IOException | InterruptedException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public CanCommitResponseProto canCommit(RpcController controller,
      CanCommitRequestProto request) throws ServiceException {
    try {
      TaskAttemptID taskId = request.hasTaskId()
          ? deserialize(new TaskAttemptID(), request.getTaskId())
          : null;
      boolean canCommit = impl.canCommit(taskId);
      return CanCommitResponseProto.newBuilder()
          .setCanCommit(canCommit)
          .build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ShuffleErrorResponseProto shuffleError(RpcController controller,
      ShuffleErrorRequestProto request) throws ServiceException {
    try {
      TaskAttemptID taskId = request.hasTaskId()
          ? deserialize(new TaskAttemptID(), request.getTaskId())
          : null;
      impl.shuffleError(taskId, request.getMessage());
      return VOID_SHUFFLE_ERROR_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public FsErrorResponseProto fsError(RpcController controller,
      FsErrorRequestProto request) throws ServiceException {
    try {
      TaskAttemptID taskId = request.hasTaskId()
          ? deserialize(new TaskAttemptID(), request.getTaskId())
          : null;
      impl.fsError(taskId, request.getMessage());
      return VOID_FS_ERROR_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public FatalErrorResponseProto fatalError(RpcController controller,
      FatalErrorRequestProto request) throws ServiceException {
    try {
      TaskAttemptID taskId = request.hasTaskId()
          ? deserialize(new TaskAttemptID(), request.getTaskId())
          : null;
      impl.fatalError(taskId, request.getMessage(), request.getFastFail());
      return VOID_FATAL_ERROR_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetMapCompletionEventsResponseProto getMapCompletionEvents(
      RpcController controller,
      GetMapCompletionEventsRequestProto request) throws ServiceException {
    try {
      JobID jobId = request.hasJobId()
          ? deserialize(new JobID(), request.getJobId())
          : null;
      TaskAttemptID taskAttemptId = request.hasTaskAttemptId()
          ? deserialize(new TaskAttemptID(), request.getTaskAttemptId())
          : null;
      MapTaskCompletionEventsUpdate update = impl.getMapCompletionEvents(
          jobId, request.getFromIndex(), request.getMaxLocs(), taskAttemptId);
      GetMapCompletionEventsResponseProto.Builder builder =
          GetMapCompletionEventsResponseProto.newBuilder();
      if (update != null) {
        builder.setEventsUpdate(serialize(update));
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public PreemptedResponseProto preempted(RpcController controller,
      PreemptedRequestProto request) throws ServiceException {
    try {
      TaskAttemptID taskId = request.hasTaskId()
          ? deserialize(new TaskAttemptID(), request.getTaskId())
          : null;
      TaskStatus taskStatus = null;
      if (request.hasTaskStatus()) {
        taskStatus = deserializeTaskStatus(request.getTaskStatus());
      }
      impl.preempted(taskId, taskStatus);
      return VOID_PREEMPTED_RESPONSE;
    } catch (IOException | InterruptedException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetCheckpointIDResponseProto getCheckpointID(RpcController controller,
      GetCheckpointIDRequestProto request) throws ServiceException {
    try {
      TaskID taskId = request.hasTaskId()
          ? deserialize(new TaskID(), request.getTaskId())
          : null;
      TaskCheckpointID checkpointID = impl.getCheckpointID(taskId);
      GetCheckpointIDResponseProto.Builder builder =
          GetCheckpointIDResponseProto.newBuilder();
      if (checkpointID != null) {
        builder.setCheckpointId(serialize(checkpointID));
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SetCheckpointIDResponseProto setCheckpointID(RpcController controller,
      SetCheckpointIDRequestProto request) throws ServiceException {
    try {
      TaskID taskId = request.hasTaskId()
          ? deserialize(new TaskID(), request.getTaskId())
          : null;
      TaskCheckpointID checkpointID = null;
      if (request.hasCheckpointId()) {
        checkpointID = deserialize(new TaskCheckpointID(),
            request.getCheckpointId());
      }
      impl.setCheckpointID(taskId, checkpointID);
      return VOID_SET_CHECKPOINT_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /*
  Serialization helpers
   */

  /**
   * Serialize a Writable into a protobuf ByteString.
   * @param writable source
   * @return data written to bytes.
   * @throws IOException write failure.
   */
  static ByteString serialize(Writable writable)
      throws IOException {
    DataOutputBuffer dob = new DataOutputBuffer();
    writable.write(dob);
    return ByteString.copyFrom(dob.getData(), 0, dob.getLength());
  }

  /**
   * Deserialize specific writable from bytes in a protobuf string.
   * @param writable the writable instance which will be filled with the data
   * @param bytes bytes to unmarshall.
   * @return the writable.
   * @param <T> type of the writable.
   * @throws IOException read failure.
   */
  static <T extends Writable> T deserialize(
      T writable, ByteString bytes) throws IOException {
    DataInputBuffer dib = new DataInputBuffer();
    byte[] b = bytes.toByteArray();
    dib.reset(b, b.length);
    writable.readFields(dib);
    return writable;
  }

  /**
   * Serialize a task status.
   * @param status task to serialize
   * @return the serialized status, including a flag to indicate status type.
   * @throws IOException write failre.
   */
  static ByteString serializeTaskStatus(TaskStatus status) throws IOException {
    DataOutputBuffer dob = new DataOutputBuffer();
    TaskStatus.writeTaskStatusForPB(dob, status);
    return ByteString.copyFrom(dob.getData(), 0, dob.getLength());
  }

  /**
   * Deserialize a byte string to a matching TaskStatus implementation.
   * @param bytes source bytes
   * @return task status
   * @throws IOException read failure.
   */
  static TaskStatus deserializeTaskStatus(ByteString bytes) throws IOException {
    DataInputBuffer dib = new DataInputBuffer();
    byte[] b = bytes.toByteArray();
    dib.reset(b, b.length);
    return TaskStatus.readTaskStatusFromPB(dib);
  }
}
