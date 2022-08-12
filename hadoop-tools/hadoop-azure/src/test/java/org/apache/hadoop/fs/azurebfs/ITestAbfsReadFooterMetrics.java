package org.apache.hadoop.fs.azurebfs;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IOSTATISTICS_LOGGING_LEVEL_INFO;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_TRACINGMETRICHEADER_FORMAT;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MIN_BUFFER_SIZE;
import org.junit.Test;
import java.util.Random;
import java.util.List;
import org.apache.hadoop.fs.azurebfs.services.AbfsReadFooterMetrics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderFormat;
import org.apache.hadoop.fs.statistics.IOStatisticsLogging;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;

public class ITestAbfsReadFooterMetrics extends AbstractAbfsScaleTest {

  public ITestAbfsReadFooterMetrics() throws Exception {
  }

  private static final String TEST_PATH = "/testfile";

  @Test
  public void testReadFooterMetrics() throws Exception {
    int bufferSize = MIN_BUFFER_SIZE;
    final AzureBlobFileSystem fs = getFileSystem();
    final AbfsConfiguration abfsConfiguration = fs.getAbfsStore().getAbfsConfiguration();
    abfsConfiguration.set(FS_AZURE_TRACINGMETRICHEADER_FORMAT, String.valueOf(TracingHeaderFormat.INTERNAL_FOOTER_METRIC_FORMAT));
    abfsConfiguration.setWriteBufferSize(bufferSize);
    abfsConfiguration.setReadBufferSize(bufferSize);

    final byte[] b = new byte[2 * bufferSize];
    new Random().nextBytes(b);

    Path testPath = path(TEST_PATH);
    FSDataOutputStream stream = fs.create(testPath);
    try {
      stream.write(b);
    } finally{
      stream.close();
    }
    IOStatisticsLogging.logIOStatisticsAtLevel(LOG, IOSTATISTICS_LOGGING_LEVEL_INFO, stream);

    final byte[] readBuffer = new byte[2 * bufferSize];
    int result;
    IOStatisticsSource statisticsSource = null;
    try (FSDataInputStream inputStream = fs.open(testPath)) {
      statisticsSource = inputStream;
      ((AbfsInputStream) inputStream.getWrappedStream()).registerListener(
          new TracingHeaderValidator(abfsConfiguration.getClientCorrelationId(),
              fs.getFileSystemId(), FSOperationType.READ, true, 0,
              ((AbfsInputStream) inputStream.getWrappedStream())
                  .getStreamID()));
      inputStream.seek(bufferSize);
      result = inputStream.read(readBuffer, bufferSize, bufferSize);
      assertNotEquals(-1, result);

      //to test tracingHeader for case with bypassReadAhead == true
      inputStream.seek(0);
      byte[] temp = new byte[5];
      int t = inputStream.read(temp, 0, 1);

      inputStream.seek(0);
      result = inputStream.read(readBuffer, 0, bufferSize);
    }
    IOStatisticsLogging.logIOStatisticsAtLevel(LOG, IOSTATISTICS_LOGGING_LEVEL_INFO, statisticsSource);

    assertNotEquals("data read in final read()", -1, result);
    assertArrayEquals(readBuffer, b);
    List<AbfsReadFooterMetrics> abfsReadFooterMetricsList = fs.getAbfsClient().getAbfsCounters().getAbfsReadFooterMetrics();
    String footerMetric = AbfsReadFooterMetrics.getFooterMetrics(abfsReadFooterMetricsList, "");
    assertEquals("NonParquet: #FR=16384.000_16384.000 #SR=1.000_16384.000 #FL=32768.000 #RL=16384.000 ", footerMetric);
  }
}
