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
package org.apache.hadoop.hdfs.tools.offlineEditsViewer;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream;

import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;

/**
 * OfflineEditsLoader walks an EditsVisitor over an EditLogInputStream
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
interface OfflineEditsLoader {
  
  abstract public void loadEdits() throws IOException;
  
  static class OfflineEditsLoaderFactory {
    static OfflineEditsLoader createLoader(OfflineEditsVisitor visitor,
        String inputFileName, boolean xmlInput,
        OfflineEditsViewer.Flags flags) throws IOException {
      if (xmlInput) {
        return new OfflineEditsXmlLoader(visitor, new File(inputFileName), flags);
      } else {
        File file = null;
        EditLogInputStream elis = null;
        OfflineEditsLoader loader = null;
        try {
          file = new File(inputFileName);
          elis = new EditLogFileInputStream(file, HdfsServerConstants.INVALID_TXID,
              HdfsServerConstants.INVALID_TXID, false);
          loader = new OfflineEditsBinaryLoader(visitor, elis, flags);
        } finally {
          if ((loader == null) && (elis != null)) {
            elis.close();
          }
        }
        return loader;
      }
    }
  }
}
