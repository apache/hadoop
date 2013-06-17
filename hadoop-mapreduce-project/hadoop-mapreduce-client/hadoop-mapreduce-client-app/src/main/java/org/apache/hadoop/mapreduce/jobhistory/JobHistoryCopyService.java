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

package org.apache.hadoop.mapreduce.jobhistory;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

/**
 * Reads in history events from the JobHistoryFile and sends them out again
 * to be recorded.
 */
public class JobHistoryCopyService extends CompositeService implements HistoryEventHandler {

  private static final Log LOG = LogFactory.getLog(JobHistoryCopyService.class);

  private final ApplicationAttemptId applicationAttemptId;
  private final EventHandler handler;
  private final JobId jobId;


  public JobHistoryCopyService(ApplicationAttemptId applicationAttemptId, 
      EventHandler handler) {
    super("JobHistoryCopyService");
    this.applicationAttemptId = applicationAttemptId;
    this.jobId =  TypeConverter.toYarn(
        TypeConverter.fromYarn(applicationAttemptId.getApplicationId()));
    this.handler = handler;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
  }
  
  @Override
  public void handleEvent(HistoryEvent event) throws IOException {
    //Skip over the AM Events this is handled elsewhere
    if (!(event instanceof AMStartedEvent)) {
      handler.handle(new JobHistoryEvent(jobId, event));
    }
  }
  
  @Override
  protected void serviceStart() throws Exception {
    try {
      //TODO should we parse on a background thread???
      parse();
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
    super.serviceStart();
  }
  
  private void parse() throws IOException {
    FSDataInputStream in = null;
    try {
      in =  getPreviousJobHistoryFileStream(getConfig(), applicationAttemptId);
    } catch (IOException e) {
      LOG.warn("error trying to open previous history file. No history data " +
      		"will be copied over.", e);
      return;
    }
    JobHistoryParser parser = new JobHistoryParser(in);
    parser.parse(this);
    Exception parseException = parser.getParseException();
    if (parseException != null) {
      LOG.info("Got an error parsing job-history file" + 
          ", ignoring incomplete events.", parseException);
    }
  }

  public static FSDataInputStream getPreviousJobHistoryFileStream(
      Configuration conf, ApplicationAttemptId applicationAttemptId)
      throws IOException {
    FSDataInputStream in = null;
    Path historyFile = null;
    String jobId =
        TypeConverter.fromYarn(applicationAttemptId.getApplicationId())
          .toString();
    String jobhistoryDir =
        JobHistoryUtils.getConfiguredHistoryStagingDirPrefix(conf, jobId);
    Path histDirPath =
        FileContext.getFileContext(conf).makeQualified(new Path(jobhistoryDir));
    FileContext fc = FileContext.getFileContext(histDirPath.toUri(), conf);
    // read the previous history file
    historyFile =
        fc.makeQualified(JobHistoryUtils.getStagingJobHistoryFile(histDirPath,
          jobId, (applicationAttemptId.getAttemptId() - 1)));
    LOG.info("History file is at " + historyFile);
    in = fc.open(historyFile);
    return in;
  }
  
  
}
