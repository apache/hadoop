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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;


import org.mockito.Mockito;

import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.services.BlobProperty;
import org.apache.hadoop.fs.azurebfs.services.PrefixMode;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class BlobDirectoryStateHelper {

  /**
   * To assert that a path exists as implicit directory we need two things to assert.
   * 1. List blobs on the path should return some entries.
   * 2. GetBlobProperties on path should fail.
   * @param path to be checked
   * @param fs AzureBlobFileSystem for API calls
   * @return boolean whether the path exists as Implicit directory or not
   */
  public static boolean isImplicitDirectory(Path path, AzureBlobFileSystem fs) throws Exception {
    path = new Path(fs.makeQualified(path).toUri().getPath());
    if (fs.getAbfsStore().getPrefixMode() == PrefixMode.BLOB) {
      List<BlobProperty> blobProperties = fs.getAbfsStore()
        .getListBlobs(path,null, Mockito.mock(TracingContext.class), 2, true);
      if (blobProperties.size() == 0) {
        return false;
      }
      try {
        fs.getAbfsStore().getBlobProperty(
            path,
            Mockito.mock(TracingContext.class)
        );
      }
      catch (AbfsRestOperationException ex) {
        if (ex.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
          return true;
        }
      }
      return false;
    }
    else {
      FileStatus[] statuses = fs.getAbfsStore()
          .listStatus(path, Mockito.mock(TracingContext.class));
      if (statuses.length == 0) {
        return false;
      }
      try {
        FileStatus status = fs.getAbfsStore().getFileStatus(
            path,
            Mockito.mock(TracingContext.class)
        );
        return !status.isDirectory();
      }
      catch (AbfsRestOperationException ex) {
        if (ex.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * To assert that a path exists as explicit directory
   * For PrefixMode Blob: GetBlobProperties on path should succeed and marker should be present
   * For PrefixMode DFS: GetFileStatus on path should succeed and marker should be present
   * @param path to be checked
   * @param fs AzureBlobFileSystem for API calls
   * @return boolean whether the path exists as Implicit directory or not
   */
  public static boolean isExplicitDirectory(Path path, AzureBlobFileSystem fs) {
    path = new Path(fs.makeQualified(path).toUri().getPath());
    if (fs.getAbfsStore().getPrefixMode() == PrefixMode.BLOB) {
      BlobProperty prop;
      try {
        prop = fs.getAbfsStore().getBlobProperty(
            path,
            Mockito.mock(TracingContext.class)
        );
      }
      catch(AzureBlobFileSystemException ex) {
        return false;
      }
      return prop.getIsDirectory();
    }
    else {
      FileStatus status;
      try {
        status = fs.getAbfsStore()
            .getFileStatus(path, Mockito.mock(TracingContext.class));
      }
      catch (IOException ex) {
        return false;
      }
      return status.isDirectory();
    }
  }
}