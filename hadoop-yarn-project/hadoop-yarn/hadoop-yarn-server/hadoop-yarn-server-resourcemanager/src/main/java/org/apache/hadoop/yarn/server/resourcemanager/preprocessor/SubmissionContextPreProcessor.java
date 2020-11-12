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

package org.apache.hadoop.yarn.server.resourcemanager.preprocessor;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Pre process the ApplicationSubmissionContext with server side info.
 */
public class SubmissionContextPreProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(
      SubmissionContextPreProcessor.class);
  private static final String DEFAULT_COMMANDS = "*";
  private static final int INITIAL_DELAY = 1000;

  enum ContextProp {
    // Node label Expression
    NL(new NodeLabelProcessor()),
    // Queue
    Q(new QueueProcessor()),
    // Tag Add
    TA(new TagAddProcessor());

    private ContextProcessor cp;
    ContextProp(ContextProcessor cp) {
      this.cp = cp;
    }
  }

  private String hostsFilePath;
  private volatile long lastModified = -1;
  private volatile Map<String, Map<ContextProp, String>> hostCommands =
      new HashMap<>();
  private ScheduledExecutorService executorService;

  public void start(Configuration conf) {
    this.hostsFilePath =
        conf.get(
            YarnConfiguration.RM_SUBMISSION_PREPROCESSOR_FILE_PATH,
            YarnConfiguration.DEFAULT_RM_SUBMISSION_PREPROCESSOR_FILE_PATH);
    int refreshPeriod =
        conf.getInt(
            YarnConfiguration.RM_SUBMISSION_PREPROCESSOR_REFRESH_INTERVAL_MS,
            YarnConfiguration.
                DEFAULT_RM_SUBMISSION_PREPROCESSOR_REFRESH_INTERVAL_MS);

    LOG.info("Submission Context Preprocessor enabled: file=[{}], "
            + "interval=[{}]", this.hostsFilePath, refreshPeriod);

    executorService = Executors.newSingleThreadScheduledExecutor();
    Runnable refreshConf = new Runnable() {
      @Override
      public void run() {
        try {
          refresh();
        } catch (Exception ex) {
          LOG.error("Error while refreshing Submission PreProcessor file [{}]",
              hostsFilePath, ex);
        }
      }
    };
    if (refreshPeriod > 0) {
      executorService.scheduleAtFixedRate(refreshConf, INITIAL_DELAY,
          refreshPeriod, TimeUnit.MILLISECONDS);
    } else {
      executorService.schedule(refreshConf, INITIAL_DELAY,
          TimeUnit.MILLISECONDS);
    }
  }

  public void stop() {
    if (this.executorService != null) {
      this.executorService.shutdownNow();
    }
  }

  public void preProcess(String host, ApplicationId applicationId,
      ApplicationSubmissionContext submissionContext) {
    Map<ContextProp, String> cMap = hostCommands.get(host);

    // Try regex match
    if (cMap == null) {
      for (Map.Entry<String, Map<ContextProp, String>> entry :
          hostCommands.entrySet()) {
        if (entry.getKey().equals(DEFAULT_COMMANDS)) {
          continue;
        }
        try {
          Pattern p = Pattern.compile(entry.getKey());
          Matcher m = p.matcher(host);
          if (m.find()) {
            cMap = hostCommands.get(entry.getKey());
          }
        } catch (PatternSyntaxException exception) {
          LOG.warn("Invalid regex pattern: " + entry.getKey());
        }
      }
    }
    // Set to default value
    if (cMap == null) {
      cMap = hostCommands.get(DEFAULT_COMMANDS);
    }
    if (cMap != null) {
      for (Map.Entry<ContextProp, String> entry : cMap.entrySet()) {
        entry.getKey().cp.process(host, entry.getValue(),
            applicationId, submissionContext);
      }
    }
  }

  @VisibleForTesting
  public void refresh() throws Exception {
    if (null == hostsFilePath || hostsFilePath.isEmpty()) {
      LOG.warn("Host list file path [{}] is empty or does not exist !!",
          hostsFilePath);
    } else {
      File hostFile = new File(hostsFilePath);
      if (!hostFile.exists() || !hostFile.isFile()) {
        LOG.warn("Host list file [{}] does not exist or is not a file !!",
            hostFile);
      } else if (hostFile.lastModified() <= lastModified) {
        LOG.debug("Host list file [{}] has not been modified from last refresh",
            hostFile);
      } else {
        FileInputStream fileInputStream = new FileInputStream(hostFile);
        BufferedReader reader = null;
        Map<String, Map<ContextProp, String>> tempHostCommands =
            new HashMap<>();
        try {
          reader = new BufferedReader(new InputStreamReader(fileInputStream,
              StandardCharsets.UTF_8));
          String line;
          while ((line = reader.readLine()) != null) {
            // Lines should start with hostname and be followed with commands.
            // Delimiter is any contiguous sequence of space or tab character.
            // Commands are of the form:
            //   <KEY>=<VALUE>
            //   where KEY can be 'NL', 'Q' or 'TA' (more can be added later)
            //   (TA stands for 'Tag Add')
            // Sample lines:
            // ...
            // host1  NL=foo   Q=b
            // host2   Q=c NL=bar
            // ...
            String[] commands = line.split("[ \t\n\f\r]+");
            if (commands != null && commands.length > 1) {
              String host = commands[0].trim();
              if (host.startsWith("#")) {
                // All lines starting with # is a comment
                continue;
              }
              Map<ContextProp, String> cMap = null;
              for (int i = 1; i < commands.length; i++) {
                String[] cSplit = commands[i].split("=");
                if (cSplit == null || cSplit.length != 2) {
                  LOG.error("No commands found for line [{}]", commands[i]);
                  continue;
                }
                if (cMap == null) {
                  cMap = new HashMap<>();
                }
                cMap.put(ContextProp.valueOf(cSplit[0]), cSplit[1]);
              }
              if (cMap != null && cMap.size() > 0) {
                tempHostCommands.put(host, cMap);
                LOG.info("Following commands registered for host[{}] : {}",
                    host, cMap);
              }
            }
          }
          lastModified = hostFile.lastModified();
        } catch (Exception ex) {
          // Do not commit the new map if we have an Exception..
          tempHostCommands = null;
          throw ex;
        } finally {
          if (tempHostCommands != null && tempHostCommands.size() > 0) {
            hostCommands = tempHostCommands;
          }
          IOUtils.cleanupWithLogger(LOG, reader, fileInputStream);
        }
      }
    }
  }
}
