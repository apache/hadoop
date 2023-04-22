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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.fs.FileStatus;

/**
 * Class to store listStatus results for AbfsListStatusRemoteIterator. The
 * results can either be of type Iterator or an exception thrown during the
 * operation
 */
public class AbfsListResult {
  private IOException listException = null;

  private Iterator<FileStatus> fileStatusIterator
      = Collections.emptyIterator();

  AbfsListResult(IOException ex) {
    this.listException = ex;
  }

  AbfsListResult(Iterator<FileStatus> fileStatusIterator) {
    this.fileStatusIterator = fileStatusIterator;
  }

  IOException getListingException() {
    return listException;
  }

  Iterator<FileStatus> getFileStatusIterator() {
    return fileStatusIterator;
  }

  boolean isFailedListing() {
    return (listException != null);
  }
}
