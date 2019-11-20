package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.hdfs.protocol.SyncTaskStats.Metrics;
import org.apache.hadoop.hdfs.server.protocol.BlockSyncTaskExecutionFeedback;
import org.apache.hadoop.hdfs.server.protocol.MetadataSyncTaskExecutionFeedback;
import org.apache.hadoop.hdfs.server.protocol.SyncTaskExecutionResult;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hdfs.server.protocol.SyncTaskExecutionOutcome.EXCEPTION;
import static org.apache.hadoop.hdfs.server.protocol.SyncTaskExecutionOutcome.FINISHED_SUCCESSFULLY;
import static org.assertj.core.api.Assertions.assertThat;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestSyncTaskStats {

  @Mock
  private MetadataSyncTaskExecutionFeedback metadataFeedbackMock;

  @Mock
  private BlockSyncTaskExecutionFeedback blockFeedbackMock;

  @Test
  public void testFrom() {
    when(metadataFeedbackMock.getOutcome()).thenReturn(FINISHED_SUCCESSFULLY);
    when(metadataFeedbackMock.getResult()).thenReturn(SyncTaskExecutionResult.emptyResult());
    when(metadataFeedbackMock.getOperation()).thenReturn(MetadataSyncTaskOperation.CREATE_DIRECTORY);

    SyncTaskStats syncTaskStats = SyncTaskStats.from(metadataFeedbackMock);

    assertThat(syncTaskStats.metaFailures).isEmpty();
    assertThat(syncTaskStats.metaSuccesses).hasSize(1);
    assertThat(syncTaskStats.metaSuccesses).containsOnlyKeys(MetadataSyncTaskOperation.CREATE_DIRECTORY);
    assertThat(syncTaskStats.metaSuccesses).containsValue(Metrics.of(1,0L));
  }


  @Test
  public void testFromWithBytes() {
    long numberOfBytes = 42L;
    when(blockFeedbackMock.getOutcome()).thenReturn(FINISHED_SUCCESSFULLY);
    when(blockFeedbackMock.getResult()).thenReturn(
        new SyncTaskExecutionResult(null, numberOfBytes));

    SyncTaskStats syncTaskStats = SyncTaskStats.from(blockFeedbackMock);

    assertThat(syncTaskStats.blockSuccesses).isEqualTo(Metrics.of(1,numberOfBytes));
  }

  @Test
  public void testFromFailure() {
    when(metadataFeedbackMock.getOutcome()).thenReturn(EXCEPTION);
    when(metadataFeedbackMock.getResult()).thenReturn(
        new SyncTaskExecutionResult(null, 0L));
    when(metadataFeedbackMock.getOperation()).thenReturn(MetadataSyncTaskOperation.MULTIPART_COMPLETE);

    SyncTaskStats syncTaskStats = SyncTaskStats.from(metadataFeedbackMock);

    assertThat(syncTaskStats.metaSuccesses).isEmpty();
    assertThat(syncTaskStats.metaFailures).hasSize(1);
    assertThat(syncTaskStats.metaFailures).containsOnlyKeys(MetadataSyncTaskOperation.MULTIPART_COMPLETE);
    assertThat(syncTaskStats.metaFailures).containsValue(1);
  }

  @Test
  public void testAppend() {
    final int leftCompletes = 4;
    final int leftCreateDirectories = 7;
    final long leftSize = 0L;

    Map<MetadataSyncTaskOperation, Metrics> successesLeft = new HashMap<>();
    successesLeft.put(MetadataSyncTaskOperation.RENAME_FILE,
        Metrics.of(leftCreateDirectories, leftSize));
    Map<MetadataSyncTaskOperation, Integer> failuresLeft = new HashMap<>();
    failuresLeft.put(MetadataSyncTaskOperation.MULTIPART_COMPLETE, leftCompletes);

    Metrics blockSuccessesLeft = Metrics.of(leftCreateDirectories, leftSize);
    int blockFailuresLeft = leftCompletes;

    SyncTaskStats left = new SyncTaskStats(successesLeft, failuresLeft,
        blockSuccessesLeft, blockFailuresLeft);

    final int rightCompletes = 89;
    final int rightCreateDirectories = 43;
    final long rightSize = 1070862354L;

    Map<MetadataSyncTaskOperation, Metrics> successesRight = new HashMap<>();
    successesRight.put(MetadataSyncTaskOperation.RENAME_FILE,
        Metrics.of(rightCreateDirectories, rightSize));
    Map<MetadataSyncTaskOperation, Integer> failuresRight = new HashMap<>();
    failuresRight.put(MetadataSyncTaskOperation.MULTIPART_COMPLETE, rightCompletes);

    Metrics blockSuccessesRight = Metrics.of(rightCreateDirectories, rightSize);
    int blockFailuresRight = rightCompletes;

    SyncTaskStats right = new SyncTaskStats(successesRight, failuresRight,
        blockSuccessesRight, blockFailuresRight);

    SyncTaskStats actual = SyncTaskStats.append(left, right);

    int actualCompletes = leftCompletes + rightCompletes;
    long actualSize = leftSize + rightSize;
    int actualCreateDirectories = leftCreateDirectories + rightCreateDirectories;

    assertThat(actual.metaSuccesses)
        .containsOnlyKeys(MetadataSyncTaskOperation.RENAME_FILE);
    assertThat(actual.metaSuccesses)
        .containsValue(Metrics.of(actualCreateDirectories, actualSize ));

    assertThat(actual.metaFailures)
        .containsOnlyKeys(MetadataSyncTaskOperation.MULTIPART_COMPLETE);
    assertThat(actual.metaFailures)
        .containsValue(actualCompletes);

    assertThat(actual.blockSuccesses)
        .isEqualTo(Metrics.of(actualCreateDirectories, actualSize));
    assertThat(actual.blockFailures)
        .isEqualTo(actualCompletes);

    String prettilyPrinted = actual.prettyPrint();
    System.out.println(prettilyPrinted);
  }

}