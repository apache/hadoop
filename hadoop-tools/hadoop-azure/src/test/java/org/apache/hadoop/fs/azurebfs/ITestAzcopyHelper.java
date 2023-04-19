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

package org.apache.hadoop.fs.azurebfs;

import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class ITestAzcopyHelper {
    public void createFileUsingAzcopy(String pathFromContainerRoot) throws IOException, InterruptedException {
        // Find the hadoop-azure directory from the current working directory
        File currentDir = new File(System.getProperty("user.dir"));
        File hadoopAzureDir;
        if (!currentDir.isDirectory() && !currentDir.getName().equals("hadoop-azure")) {
            hadoopAzureDir = findHadoopAzureDir(currentDir);
            if (hadoopAzureDir == null) {
                throw new FileNotFoundException("hadoop-azure directory not found");
            }
        }

        // Check if azcopy is present in the hadoop-azure directory
        String azcopyPath = hadoopAzureDir.getAbsolutePath() + "/azcopy/azcopy";
        File azcopyFile = new File(azcopyPath);
        if (!azcopyFile.exists()) {
            // If azcopy is not present, download it
            String downloadUrl = "https://aka.ms/downloadazcopy-v10-linux";
            String downloadCmd = "wget " + downloadUrl + " -O azcopy.tar.gz && tar -xf azcopy.tar.gz && rm azcopy.tar.gz";
            String[] downloadCmdArr = { "bash", "-c", downloadCmd };
            Process downloadProcess = Runtime.getRuntime().exec(downloadCmdArr);
            downloadProcess.waitFor();
            azcopyPath = "./azcopy";
        }

        // Change working directory to the hadoop-azure directory
        System.setProperty("user.dir", hadoopAzureDir.getAbsolutePath());
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
            if (file.isDirectory() && file.getName().equals("hadoop-azure")) {
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

    @Test
    public void testAzcopyDownload() throws IOException, InterruptedException {
        createFileUsingAzcopy("abcd");
    }
}

