package org.apache.hadoop.yarn.logaggregation.testutils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogDeletionService;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.yarn.logaggregation.testutils.MockRMClientUtils.createMockRMClient;

public class AggregatedLogDeletionServiceForTest extends AggregatedLogDeletionService {
  private final List<ApplicationId> finishedApplications;
  private final List<ApplicationId> runningApplications;
  private final Configuration conf;

  public AggregatedLogDeletionServiceForTest(List<ApplicationId> runningApplications,
                                             List<ApplicationId> finishedApplications) {
    this(runningApplications, finishedApplications, null);
  }

  public AggregatedLogDeletionServiceForTest(List<ApplicationId> runningApplications,
                                             List<ApplicationId> finishedApplications,
                                             Configuration conf) {
    this.runningApplications = runningApplications;
    this.finishedApplications = finishedApplications;
    this.conf = conf;
  }

  @Override
  protected ApplicationClientProtocol createRMClient() throws IOException {
    try {
      return createMockRMClient(finishedApplications, runningApplications);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  protected Configuration createConf() {
    return conf;
  }

  @Override
  protected void stopRMClient() {
    // DO NOTHING
  }
}
