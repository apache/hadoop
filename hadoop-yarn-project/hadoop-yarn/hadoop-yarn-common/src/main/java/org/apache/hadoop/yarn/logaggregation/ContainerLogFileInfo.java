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

package org.apache.hadoop.yarn.logaggregation;

/**
 * ContainerLogFileInfo represents the meta data for a container log file,
 * which includes:
 * <ul>
 *   <li>The filename of the container log.</li>
 *   <li>The size of the container log.</li>
 *   <li>The last modification time of the container log.</li>
 * </ul>
 *
 */
public class ContainerLogFileInfo {
  private String fileName;
  private String fileSize;
  private String lastModifiedTime;

  //JAXB needs this
  public ContainerLogFileInfo() {}

  public ContainerLogFileInfo(String fileName, String fileSize,
      String lastModifiedTime) {
    this.setFileName(fileName);
    this.setFileSize(fileSize);
    this.setLastModifiedTime(lastModifiedTime);
  }

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public String getFileSize() {
    return fileSize;
  }

  public void setFileSize(String fileSize) {
    this.fileSize = fileSize;
  }

  public String getLastModifiedTime() {
    return lastModifiedTime;
  }

  public void setLastModifiedTime(String lastModifiedTime) {
    this.lastModifiedTime = lastModifiedTime;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((fileName == null) ? 0 : fileName.hashCode());
    result = prime * result + ((fileSize == null) ? 0 : fileSize.hashCode());
    result = prime * result + ((lastModifiedTime == null) ?
        0 : lastModifiedTime.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object otherObj) {
    if (otherObj == this) {
      return true;
    }
    if (!(otherObj instanceof ContainerLogFileInfo)) {
      return false;
    }
    ContainerLogFileInfo other = (ContainerLogFileInfo)otherObj;
    return other.fileName.equals(fileName) && other.fileSize.equals(fileSize)
        && other.lastModifiedTime.equals(lastModifiedTime);
  }
}
