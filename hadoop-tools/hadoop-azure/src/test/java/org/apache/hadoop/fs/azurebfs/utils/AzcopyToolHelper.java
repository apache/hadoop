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

package org.apache.hadoop.fs.azurebfs.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DOT;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FORWARD_SLASH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.SINGLE_WHITE_SPACE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_BLOB_DOMAIN_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_DFS_DOMAIN_NAME;

/**
 * Singleton class to create a file or folder in Azure Blob Storage using Azcopy tool.
 * <a href="https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-v10">
 * Azcopy</a> is a command-line utility tool to copy blobs or files to or from a storage account.
 * It uses Blob Endpoint and ends up creating implicit paths in the storage account.
 * We will leverage this tool to create implicit paths in storage account for testing purposes.
 */
public final class AzcopyToolHelper {

  private File hadoopAzureDir;
  private String azcopyDirPath;
  private String azcopyExecutablePath;
  private String fileCreationScriptPath;
  private String folderCreationScriptPath;
  private String fileCreationScriptContent;
  private String folderCreationScriptContent;
  private String sasToken;
  private boolean initialized = false;

  private static final String USER_DIR_SYSTEM_PROPERTY = "user.dir";
  private static final String HADOOP_AZURE_DIR = "hadoop-azure";
  private static final String AZCOPY_DIR_NAME = "azcopy";
  private static final String AZCOPY_EXECUTABLE_NAME = "azcopy";
  private static final String FILE_CREATION_SCRIPT_NAME = "createAzcopyFile.sh";
  private static final String FOLDER_CREATION_SCRIPT_NAME = "createAzcopyFolder.sh";
  private static final String DIR_NOT_FOUND_ERROR = " directory not found";
  private static final String AZCOPY_DOWNLOADED_DIR_NAME = "/azcopy_linux_amd64_*/* ";
  private static final String AZCOPY_DOWNLOADED_TAR_NAME = "/azcopy_linux_amd64_* azcopy.tar.gz";
  private static final String SCRIPT_CREATION_ERROR = "Unable to create azcopy file/folder creation script. ";
  private static final String SCRIPT_RUN_ERROR = "Unable to run azcopy file/folder creation script. ";
  private static final String SCRIPT_NOT_FOUND_ERROR = "Azcopy file/folder creation script not found and should be regenerated. ";
  private static final String SCRIPT_EXECUTION_FAILED = "Azcopy file/folder creation script failed with non-zero exit code. "
      + "This can be due to corrupt azcopy executable or invalid SAS Token. Exit code: ";

  private static final String AZCOPY_CMD_SHELL = "bash";
  private static final String AZCOPY_CMD_OPTION = "-c";
  private static final String AZCOPY_DOWNLOAD_URL = "https://aka.ms/downloadazcopy-v10-linux";
  private static final String AZCOPY_DOWNLOAD_CMD = "wget " + AZCOPY_DOWNLOAD_URL + " -O azcopy.tar.gz" + " --no-check-certificate";
  private static final String EXTRACT_CMD = "tar -xf azcopy.tar.gz -C ";
  private static final String MOVE_CMD = "mv ";
  private static final String REMOVE_CMD = "rm -rf ";
  private static final String CHMOD_CMD = "chmod +x ";
  private static final char QUESTION_MARK = '?';
  private static final int WAIT_TIME = 10_000; // 10 seconds
  private static final int MAX_WAIT_TIME = 2 * 6 * WAIT_TIME; // 2 minutes
  private static final Logger LOG = LoggerFactory.getLogger(AzcopyToolHelper.class);

  private static AzcopyToolHelper azcopyToolHelper; // singleton, initialized in static initialization block
  private static final ReentrantLock LOCK = new ReentrantLock();

  private AzcopyToolHelper() {

  }

  /**
   * Constructor to initialize the AzcopyToolHelper. Each JVM running will have
   * its own instance but will share the tool and scripts.
   * Azcopy tool work with SAS based authentication. SAS can be configured using
   * test configuration "fs.azure.test.fixed.sas.token".
   * @param sasToken to be used for authentication.
   */
  public static AzcopyToolHelper getInstance(String sasToken)
      throws IOException, InterruptedException {
    if (azcopyToolHelper == null) {
      LOCK.lock();
      try {
        if (azcopyToolHelper == null) {
          azcopyToolHelper = new AzcopyToolHelper();
          azcopyToolHelper.init(sasToken);
        }
      } finally {
        LOCK.unlock();
      }
    }
    return azcopyToolHelper;
  }

  /**
   * Create a file with implicit parent in the container using Azcopy tool.
   * @param absolutePathToBeCreated absolute path to be created.
   * @throws Exception if file creation fails.
   */
  public void createFileUsingAzcopy(String absolutePathToBeCreated) throws Exception {
    if (absolutePathToBeCreated != null) {
      absolutePathToBeCreated = absolutePathToBeCreated.replace(
          ABFS_DFS_DOMAIN_NAME, ABFS_BLOB_DOMAIN_NAME) + sasToken;
      runShellScript(fileCreationScriptPath, absolutePathToBeCreated);
    }
  }

  /**
   * Create a implicit folder with implicit parent in the container using Azcopy tool.
   * @param absolutePathToBeCreated absolute path to be created.
   * @throws Exception
   */
  public void createFolderUsingAzcopy(String absolutePathToBeCreated) throws Exception {
    if (absolutePathToBeCreated != null) {
      absolutePathToBeCreated = absolutePathToBeCreated.replace(
          ABFS_DFS_DOMAIN_NAME, ABFS_BLOB_DOMAIN_NAME) + sasToken;
      runShellScript(folderCreationScriptPath, absolutePathToBeCreated);
    }
  }

  private void init(String sasToken) throws IOException, InterruptedException {
    if (initialized) {
      return;
    }
    this.sasToken = sasToken.charAt(0) == QUESTION_MARK ? sasToken : QUESTION_MARK + sasToken;
    hadoopAzureDir = findHadoopAzureDir();
    azcopyDirPath = hadoopAzureDir.getAbsolutePath() + FORWARD_SLASH + AZCOPY_DIR_NAME;
    azcopyExecutablePath = azcopyDirPath + FORWARD_SLASH + AZCOPY_EXECUTABLE_NAME;
    fileCreationScriptPath = azcopyDirPath + FORWARD_SLASH + FILE_CREATION_SCRIPT_NAME;
    folderCreationScriptPath = azcopyDirPath + FORWARD_SLASH + FOLDER_CREATION_SCRIPT_NAME;
    fileCreationScriptContent = "blobPath=$1\n" + "echo $blobPath\n"
        + azcopyExecutablePath + " copy \"" + azcopyDirPath
        + "/NOTICE.txt\" $blobPath\n";
    folderCreationScriptContent = "blobPath=$1\n" + "echo $blobPath\n"
        + azcopyExecutablePath + " copy \"" + azcopyDirPath
        + "\" $blobPath --recursive\n";

    /*
     * Synchronized across JVMs on directory creation. If multiple process try
     * to create directory, only one will succeed and that process will download
     * azcopy tool if not present and generate scripts if not present.
     */
    downloadAzcopyToolAndGenerateScripts();

    // Change working directory to the hadoop-azure directory.
    System.setProperty(USER_DIR_SYSTEM_PROPERTY, hadoopAzureDir.getAbsolutePath());

    initialized = true;
  }

  private void downloadAzcopyToolAndGenerateScripts()
      throws IOException, InterruptedException {
    File azcopyDir = new File(azcopyDirPath);
    if (!azcopyDir.exists()) {
      if (!azcopyDir.mkdir()) {
        LOG.info("Azcopy Directory not created by process: {}",
            Thread.currentThread().getName());
        return;
      }
      downloadAzcopyExecutable();
      createShellScript(fileCreationScriptPath, fileCreationScriptContent);
      createShellScript(folderCreationScriptPath, folderCreationScriptContent);
    } else {
      LOG.info("Azcopy directory already exists. Skipping download and script "
          + "generation by the process: {}", Thread.currentThread().getName());
    }
  }

  private void downloadAzcopyExecutable()
      throws IOException, InterruptedException {
    // Check if azcopy tool is present in the azcopy directory.
    File azcopyFile = new File(azcopyExecutablePath);
    if (!azcopyFile.exists()) {
      // Download Azcopy tool from the Azure website.
      executeCommand(AZCOPY_DOWNLOAD_CMD);

      // Extract the azcopy executable from the tarball
      String extractCmd = EXTRACT_CMD + hadoopAzureDir.getAbsolutePath();
      executeCommand(extractCmd);

      // Rename the azcopy_linux_amd64_* directory to 'azcopy'
      String renameCmd = MOVE_CMD + hadoopAzureDir.getAbsolutePath()
          + AZCOPY_DOWNLOADED_DIR_NAME + azcopyDirPath;
      executeCommand(renameCmd);

      // Remove the downloaded tarball and azcopy folder
      String cleanupCmd = REMOVE_CMD + hadoopAzureDir.getAbsolutePath()
          + AZCOPY_DOWNLOADED_TAR_NAME;
      executeCommand(cleanupCmd);

      // Set the execute permission on the azcopy executable
      String chmodCmd = CHMOD_CMD + azcopyDirPath;
      executeCommand(chmodCmd);
    } else {
      LOG.info("Azcopy executable already exists. Skipping download by process: {}",
          Thread.currentThread().getName());
    }
  }

  private void executeCommand(String command) throws IOException, InterruptedException {
    String[] commandArray = {AZCOPY_CMD_SHELL, AZCOPY_CMD_OPTION, command};
    Process process = Runtime.getRuntime().exec(commandArray);
    process.waitFor();
  }

  /**
   * Create a shell script if not already created.
   * @param scriptPath to be created
   * @param scriptContent to be written in the script.
   */
  private void createShellScript(String scriptPath, String scriptContent) throws IOException {
    File scriptFile = new File(scriptPath);
    if (!scriptFile.exists()) {
      try {
        FileWriter writer = new FileWriter(scriptFile);
        writer.write(scriptContent);
        writer.close();
        scriptFile.setExecutable(true); // make the script executable
      } catch (IOException e) {
        LOG.error("Error creating shell script: {} by process {}",
            e.getMessage(), Thread.currentThread().getName());
        throw new AzcopyExecutionException(SCRIPT_CREATION_ERROR, azcopyDirPath, e);
      }
    }
  }

  private void runShellScript(String scriptPath, String argument) throws IOException {
    // Check if script exists, otherwise wait for parallel JVM process to create the script.
    checkAndWaitOnScriptCreation(scriptPath);
    try {
      ProcessBuilder pb = new ProcessBuilder(scriptPath, argument);
      Process p = pb.start();
      // wait for the process to finish
      int exitCode = p.waitFor();
      if (exitCode != 0) {
        throw new AzcopyExecutionException(SCRIPT_EXECUTION_FAILED + exitCode
            + DOT + SINGLE_WHITE_SPACE, azcopyDirPath);
      }
    } catch (AzcopyExecutionException e) {
      throw e;
    } catch (Exception e) {
      throw new AzcopyExecutionException(SCRIPT_RUN_ERROR, azcopyDirPath, e);
    }
  }

  private void checkAndWaitOnScriptCreation(String scriptPath) throws IOException {
    File scriptFile = new File(scriptPath);
    int totalWaitTime = 0;
    while (!(scriptFile.exists() && scriptFile.canExecute())) {
      try {
        Thread.sleep(WAIT_TIME);
        totalWaitTime += WAIT_TIME;
        if (totalWaitTime > MAX_WAIT_TIME) {
          LOG.error("Timeout waiting for script creation: {} by process {}",
              scriptPath, Thread.currentThread().getName());
          throw new AzcopyExecutionException(SCRIPT_NOT_FOUND_ERROR, azcopyDirPath);
        }
      } catch (InterruptedException e) {
        LOG.error("Error waiting for script creation: {} by process {}",
            scriptPath, Thread.currentThread().getName());
        throw new AzcopyExecutionException(SCRIPT_NOT_FOUND_ERROR, azcopyDirPath);
      }
    }
  }

  private File findHadoopAzureDir() throws FileNotFoundException {
    // Find the hadoop-azure directory from the current working directory.
    File hadoopAzureDir;
    File currentDir = new File(System.getProperty(USER_DIR_SYSTEM_PROPERTY));
    if (!currentDir.isDirectory() && !currentDir.getName().equals(HADOOP_AZURE_DIR)) {
      hadoopAzureDir = findHadoopAzureDir(currentDir);
      if (hadoopAzureDir == null) {
        throw new FileNotFoundException(HADOOP_AZURE_DIR + DIR_NOT_FOUND_ERROR);
      }
    } else {
      hadoopAzureDir = currentDir;
    }
    return hadoopAzureDir;
  }

  private File findHadoopAzureDir(File dir) {
    if (dir == null) {
      return null;
    }
    File[] files = dir.listFiles();
    if (files == null) {
      return null;
    }
    for (File file : files) {
      if (file.isDirectory() && file.getName().equals(HADOOP_AZURE_DIR)) {
        return file;
      } else {
        File hadoopAzureDir = findHadoopAzureDir(file);
        if (hadoopAzureDir != null) {
          return hadoopAzureDir;
        }
      }
    }
    return null;
  }
}
