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

package org.apache.hadoop.eclipse.dfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.ui.PlatformUI;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSchException;

/**
 * File handling methods for the DFS
 */
public class DfsFile extends DfsPath {

  public DfsFile(DfsPath parent, String path) {
    super(parent, path);
  }

  /**
   * Download and view contents of a file in the DFS NOTE: may not work on
   * files >1 MB.
   * 
   * @return a FileInputStream for the file
   */
  public FileInputStream open() throws JSchException, IOException,
      InvocationTargetException, InterruptedException {

    File tempFile =
        File.createTempFile("hadoop" + System.currentTimeMillis(), "tmp");
    tempFile.deleteOnExit();

    this.downloadToLocalFile(tempFile);

    // file size greater than 1 MB
    if (tempFile.length() > 1024 * 1024) {
      boolean answer =
          MessageDialog.openQuestion(null, "Show large file from DFS?",
              "The file you are attempting to download from the DFS, "
                  + this.getPath() + " is over 1MB in size. \n"
                  + "Opening this file may cause performance problems."
                  + " You can open the file with your favourite editor at "
                  + tempFile.getAbsolutePath()
                  + " (it's already saved there)."
                  + " Continue opening the file in eclipse?");
      if (!answer) {
        return null;
      }
    }

    return new FileInputStream(tempFile);
  }

  public void downloadToLocalFile(File localFile) throws JSchException,
      IOException, InvocationTargetException, InterruptedException {

    final ChannelExec exec =

    exec(" dfs " + DfsFolder.s_whichFS + " -cat " + getPath());

    final OutputStream os =
        new BufferedOutputStream(new FileOutputStream(localFile));

    try {
      PlatformUI.getWorkbench().getProgressService().busyCursorWhile(
          new IRunnableWithProgress() {
            public void run(IProgressMonitor monitor)
                throws InvocationTargetException {
              try {
                monitor.beginTask("View file from Distributed File System",
                    IProgressMonitor.UNKNOWN);
                exec.connect();
                BufferedInputStream stream =
                    new BufferedInputStream(exec.getInputStream());

                byte[] buffer = new byte[1024];
                int bytes;

                while ((bytes = stream.read(buffer)) >= 0) {
                  if (monitor.isCanceled()) {
                    os.close();
                    return;
                  }

                  monitor.worked(1);
                  os.write(buffer, 0, bytes);
                }

                monitor.done();
              } catch (Exception e) {
                throw new InvocationTargetException(e);
              }
            }
          });
    } finally {
      if (exec.isConnected()) {
        exec.disconnect();
      }
      os.close();
    }
  }

  /* @inheritDoc */
  @Override
  public void downloadToLocalDirectory(String localDirectory)
      throws InvocationTargetException, JSchException, InterruptedException,
      IOException {

    File dir = new File(localDirectory);
    if (!dir.exists() || !dir.isDirectory())
      return; // TODO display error message

    File dfsPath = new File(this.getPath());
    File destination = new File(dir, dfsPath.getName());

    if (destination.exists()) {
      boolean answer =
          MessageDialog.openQuestion(null, "Overwrite existing local file?",
              "The file you are attempting to download from the DFS "
                  + this.getPath()
                  + ", already exists in your local directory as "
                  + destination + ".\n" + "Overwrite the existing file?");
      if (!answer)
        return;
    }

    this.downloadToLocalFile(destination);
  }

}
