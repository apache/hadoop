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
package org.apache.hadoop.mapred;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;


class IndexRecord {
  final long startOffset;
  final long rawLength;
  final long partLength;
  
  public IndexRecord(long startOffset, long rawLength, long partLength) {
    this.startOffset = startOffset;
    this.rawLength = rawLength;
    this.partLength = partLength;
  }

  public static IndexRecord[] readIndexFile(Path indexFileName, 
                                            JobConf job) 
  throws IOException {

    FileSystem  localFs = FileSystem.getLocal(job);
    FileSystem rfs = ((LocalFileSystem)localFs).getRaw();

    FSDataInputStream indexInputStream = rfs.open(indexFileName);
    long length = rfs.getFileStatus(indexFileName).getLen();
    IFileInputStream checksumIn = 
      new IFileInputStream(indexInputStream,length);

    int numEntries = (int) length/MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH;

    IndexRecord[] indexRecordArray = new IndexRecord[numEntries];
    
    DataInputStream wrapper = new DataInputStream(checksumIn);

    try {
      for (int i= 0; i < numEntries; i++) {
        long startOffset = wrapper.readLong();
        long rawLength = wrapper.readLong();
        long partLength = wrapper.readLong();
        indexRecordArray[i] = 
          new IndexRecord(startOffset, rawLength, partLength);
      }
    }
    finally {
      //This would internally call checkumIn.close
      wrapper.close();
    }
    return indexRecordArray;
  }
}
