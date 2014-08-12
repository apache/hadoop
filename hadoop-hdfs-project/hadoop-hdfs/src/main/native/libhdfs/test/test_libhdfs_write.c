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

#include "hdfs.h" 

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>

int main(int argc, char **argv) {
    hdfsFS fs;
    const char *writeFileName = argv[1];
    off_t fileTotalSize = strtoul(argv[2], NULL, 10);
    long long tmpBufferSize = strtoul(argv[3], NULL, 10);
    tSize bufferSize;
    hdfsFile writeFile;
    char* buffer;
    int i;
    off_t nrRemaining;
    tSize curSize;
    tSize written;

    if (argc != 4) {
        fprintf(stderr, "Usage: hdfs_write <filename> <filesize> <buffersize>\n");
        exit(-1);
    }
    
    fs = hdfsConnect("default", 0);
    if (!fs) {
        fprintf(stderr, "Oops! Failed to connect to hdfs!\n");
        exit(-1);
    } 

    // sanity check
    if(fileTotalSize == ULONG_MAX && errno == ERANGE) {
      fprintf(stderr, "invalid file size %s - must be <= %lu\n", argv[2], ULONG_MAX);
      exit(-3);
    }

    // currently libhdfs writes are of tSize which is int32
    if(tmpBufferSize > INT_MAX) {
      fprintf(stderr, "invalid buffer size libhdfs API write chunks must be <= %d\n",INT_MAX);
      exit(-3);
    }

    bufferSize = (tSize)tmpBufferSize;

    writeFile = hdfsOpenFile(fs, writeFileName, O_WRONLY, bufferSize, 0, 0);
    if (!writeFile) {
        fprintf(stderr, "Failed to open %s for writing!\n", writeFileName);
        exit(-2);
    }

    // data to be written to the file
    buffer = malloc(sizeof(char) * bufferSize);
    if(buffer == NULL) {
        fprintf(stderr, "Could not allocate buffer of size %d\n", bufferSize);
        return -2;
    }
    for (i=0; i < bufferSize; ++i) {
        buffer[i] = 'a' + (i%26);
    }

    // write to the file
    for (nrRemaining = fileTotalSize; nrRemaining > 0; nrRemaining -= bufferSize ) {
      curSize = ( bufferSize < nrRemaining ) ? bufferSize : (tSize)nrRemaining; 
      if ((written = hdfsWrite(fs, writeFile, (void*)buffer, curSize)) != curSize) {
        fprintf(stderr, "ERROR: hdfsWrite returned an error on write: %d\n", written);
        exit(-3);
      }
    }

    free(buffer);
    hdfsCloseFile(fs, writeFile);
    hdfsDisconnect(fs);

    return 0;
}

/**
 * vim: ts=4: sw=4: et:
 */

