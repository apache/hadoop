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

package org.apache.hadoop.mapreduce.v2;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ResourceMgrDelegate;
import org.apache.hadoop.mapred.YARNRunner;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationState;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test if the jobclient shows enough diagnostics 
 * on a job failure.
 *
 */
public class TestYARNRunner extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestYARNRunner.class);
  private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
 
  private YARNRunner yarnRunner;
  private ResourceMgrDelegate resourceMgrDelegate;
  private Configuration conf;
  private ApplicationId appId;
  private JobID jobId;
  private File testWorkDir = 
      new File("target", TestYARNRunner.class.getName());
  private ApplicationSubmissionContext submissionContext;
  private static final String failString = "Rejected job";
 
  @Before
  public void setUp() throws Exception {
    resourceMgrDelegate = mock(ResourceMgrDelegate.class);
    conf = new Configuration();
    yarnRunner = new YARNRunner(conf, resourceMgrDelegate);
    yarnRunner = spy(yarnRunner);
    submissionContext = mock(ApplicationSubmissionContext.class);
    doAnswer(
        new Answer<ApplicationSubmissionContext>() {
          @Override
          public ApplicationSubmissionContext answer(InvocationOnMock invocation)
              throws Throwable {
            return submissionContext;
          }
        }
        ).when(yarnRunner).createApplicationSubmissionContext(any(Configuration.class),
            any(String.class), any(Credentials.class));
    
    appId = recordFactory.newRecordInstance(ApplicationId.class);
    appId.setClusterTimestamp(System.currentTimeMillis());
    appId.setId(1);
    jobId = TypeConverter.fromYarn(appId);
    if (testWorkDir.exists()) {
      FileContext.getLocalFSFileContext().delete(new Path(testWorkDir.toString()), true);
    }
    testWorkDir.mkdirs();
   }
  
  
  @Test
  public void testJobSubmissionFailure() throws Exception {
    when(resourceMgrDelegate.submitApplication(any(ApplicationSubmissionContext.class))).
    thenReturn(appId);
    ApplicationReport report = mock(ApplicationReport.class);
    when(report.getApplicationId()).thenReturn(appId);
    when(report.getDiagnostics()).thenReturn(failString);
    when(report.getState()).thenReturn(ApplicationState.FAILED);
    when(resourceMgrDelegate.getApplicationReport(appId)).thenReturn(report);
    Credentials credentials = new Credentials();
    File jobxml = new File(testWorkDir, "job.xml");
    OutputStream out = new FileOutputStream(jobxml);
    conf.writeXml(out);
    out.close();
    try {
      yarnRunner.submitJob(jobId, testWorkDir.getAbsolutePath().toString(), credentials); 
    } catch(IOException io) {
      LOG.info("Logging exception:", io);
      assertTrue(io.getLocalizedMessage().contains(failString));
    }
  }
}
