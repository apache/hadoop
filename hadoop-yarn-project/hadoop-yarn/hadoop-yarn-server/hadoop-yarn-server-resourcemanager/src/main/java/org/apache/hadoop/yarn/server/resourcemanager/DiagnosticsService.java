/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager;


import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CommonIssues;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FileContent;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.IssueData;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.IssueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;

/**
 * Utility methods to launch the diagnostic scripts.
 */
@InterfaceAudience.Private
public final class DiagnosticsService {
  private static final Logger LOG = LoggerFactory
      .getLogger(DiagnosticsService.class);
  private static final String PYTHON_COMMAND = "python3";
  private static final String COLON = ":";
  private static final String COMMA = ",";
  private static final String OUT_DIR_PREFIX = "/tmp";
  private static final String EXECUTION_ERROR_MESSAGE = "Error occurred " +
      "during the execution of the diagnostic script with the command '{}'.";
  private static final String INCORRECT_NUMBER_OF_PARAMETERS_MESSAGE =
      "Error while parsing diagnostic option, incorrect number of " +
          "parameters. Expected 1 or 2, but got {}. Skipping this option.";


  private static String scriptLocation = null;

  static {
    try {
      // Extract script from JAR to a temp file
      InputStream in = DiagnosticsService.class.getClassLoader()
              .getResourceAsStream("diagnostics/diagnostics_collector.py");
      File tempScript = File.createTempFile("diagnostics_collector", ".py");
      Files.copy(in, tempScript.toPath(), StandardCopyOption.REPLACE_EXISTING);
      tempScript.setExecutable(true); // Set execute permission
      scriptLocation = tempScript.getAbsolutePath();
    } catch (IOException e) {
      LOG.error("Failed to extract Python script from JAR", e);
    }
  }

  private DiagnosticsService() {
    // hidden constructor
  }

  public static CommonIssues listCommonIssues() throws Exception {
    if (Shell.WINDOWS) {
      throw new UnsupportedOperationException("Not implemented for Windows.");
    }
    CommonIssues issueTypes = new CommonIssues();
    ProcessBuilder pb = createProcessBuilder(CommandArgument.LIST_ISSUES);

    List<String> result = executeCommand(pb);

    System.out.println("-----------------------------Common Issue Result: " + result);

    for (String line : result) {
      issueTypes.add(parseIssueType(line));
    }

    return issueTypes;
  }

  public static IssueData collectIssueData(String issueId, List<String> args)
      throws Exception {
    if (Shell.WINDOWS) {
      throw new UnsupportedOperationException("Not implemented for Windows.");
    }
    ProcessBuilder pb = createProcessBuilder(CommandArgument.COMMAND, issueId,
        args);

    LOG.info("Diagnostic process environment: {}", pb.environment());

    List<String> result = executeCommand(pb);
    Optional<String> outputDirectory = result.stream()
        .filter(e -> e.contains(OUT_DIR_PREFIX))
        .findFirst();

    if (!outputDirectory.isPresent()) {
      LOG.error(EXECUTION_ERROR_MESSAGE, pb.command());
      throw new IOException("Output directory in result not found.");
    }


    return collectIssueFilesContent(new File(outputDirectory.get()));
  }

  public static IssueData collectIssueFilesContent(File currentDir){
    IssueData issueData = new IssueData();
    File[] files = currentDir.listFiles();

    if (!currentDir.exists()){
      LOG.error("Directory does not exist: {}", currentDir);
      return issueData;
    }

    if (files == null) {
      return issueData;
    }

    for (File file : files) {
      try {
        if (file.isDirectory()) {
          issueData.getFiles().addAll(collectIssueFilesContent(file).getFiles());
        } else {
          String contentType = Files.probeContentType(file.toPath());
          if (contentType == null) contentType = "text/plain"; // Default if not found

          String content = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
          FileContent fileContent = new FileContent();
          fileContent.setFilename(currentDir.toPath().relativize(file.toPath()).toString()); // Set the relative path as name
          fileContent.setContentType(contentType);
          fileContent.setContent(content);

          issueData.getFiles().add(fileContent);
        }
      } catch (IOException e) {
        LOG.error("Failed to process {}: {}", file, e.getMessage());
      }
    }
    return issueData;
  }

  @VisibleForTesting
  protected static ProcessBuilder createProcessBuilder(CommandArgument argument) {
    return createProcessBuilder(argument, null, null);
  }

  @VisibleForTesting
  protected static ProcessBuilder createProcessBuilder(
      CommandArgument argument, String issueId, List<String> additionalArgs) {
    List<String> commandList =
        new ArrayList<>(Arrays.asList(PYTHON_COMMAND, scriptLocation,
            argument.getShortOption()));

    if (argument.equals(CommandArgument.COMMAND)) {
      commandList.add(issueId);
      if (additionalArgs != null) {
        commandList.add(CommandArgument.ARGUMENTS.getShortOption());
        commandList.addAll(additionalArgs);
      }
    }

    return new ProcessBuilder(commandList);
  }

  private static List<String> executeCommand(ProcessBuilder pb)
      throws Exception {
    Process process = pb.start();
    int exitCode;
    List<String> result = new ArrayList<>();

    try (
            BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(process.getInputStream(),
                    StandardCharsets.UTF_8));
            BufferedReader stderrReader = new BufferedReader(new InputStreamReader(process.getErrorStream(),
                    StandardCharsets.UTF_8));
      ) {

      String line;
      while ((line = stdoutReader.readLine()) != null) {
        result.add(line);
      }

      List<String> errors = new ArrayList<>();
      while ((line = stderrReader.readLine()) != null) {
        errors.add(line);
      }
      if (!errors.isEmpty()) {
          LOG.error("Python script stderr: {}", errors);
      }

      process.waitFor();
    } catch (Exception e) {
      LOG.error(EXECUTION_ERROR_MESSAGE, pb.command());
      throw e;
    }
    exitCode = process.exitValue();
    if (exitCode != 0) {
      throw new IOException("The collector script exited with non-zero " +
          "exit code: " + exitCode);
    }

    return result;
  }

  @VisibleForTesting
  protected static IssueType parseIssueType(String line) {
    String[] issueParams = line.split(COLON);
    IssueType parsedIssueType;

    if (issueParams.length < 1 || issueParams.length > 2) {
      LOG.warn(INCORRECT_NUMBER_OF_PARAMETERS_MESSAGE,
          issueParams.length);
      return null;
    } else {
      String name = issueParams[0];
      parsedIssueType = new IssueType(name);
      if (issueParams.length == 2) {
        List<String> parameterList =
            Arrays.asList(issueParams[1].split(COMMA));
        parsedIssueType.setParameters(parameterList);
      }
    }

    return parsedIssueType;
  }

  @VisibleForTesting
  protected static void setScriptLocation(String scriptLocationParam) {
    scriptLocation = scriptLocationParam;
  }

  enum CommandArgument{
    LIST_ISSUES("-l"),
    COMMAND("-c"),
    ARGUMENTS("-a");

    private final String shortOption;

    CommandArgument(String shortOption) {
      this.shortOption = shortOption;
    }

    public CommandArgument fromString(String option) {
      for (CommandArgument arg : CommandArgument.values()) {
        if (arg.shortOption.equals(option)) {
          return arg;
        }
      }
      return null;
    }

    public String getShortOption() {
      return shortOption;
    }



  }
}