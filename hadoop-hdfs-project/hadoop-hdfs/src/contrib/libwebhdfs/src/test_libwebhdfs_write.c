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
#include <string.h>

int main(int argc, char **argv) {
    
    if (argc != 6) {
        fprintf(stderr, "Usage: hdfs_write <filename> <filesize> <buffersize> <username> <append>\n");
        exit(-1);
    }
    
    hdfsFS fs = hdfsConnectAsUser("0.0.0.0", 50070, argv[4]);
    if (!fs) {
        fprintf(stderr, "Oops! Failed to connect to hdfs!\n");
        exit(-1);
    }
    
    const char* writeFileName = argv[1];
    off_t fileTotalSize = strtoul(argv[2], NULL, 10);
    long long tmpBufferSize = strtoul(argv[3], NULL, 10);
    
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
    
    tSize bufferSize = tmpBufferSize;
    
    hdfsFile writeFile = NULL;
    int append = atoi(argv[5]);
    if (!append) {
        writeFile = hdfsOpenFile(fs, writeFileName, O_WRONLY, bufferSize, 2, 0);
    } else {
        writeFile = hdfsOpenFile(fs, writeFileName, O_WRONLY | O_APPEND, bufferSize, 2, 0);
    }
    if (!writeFile) {
        fprintf(stderr, "Failed to open %s for writing!\n", writeFileName);
        exit(-2);
    }
    
    // data to be written to the file
    char* buffer = malloc(sizeof(char) * bufferSize + 1);
    if(buffer == NULL) {
        fprintf(stderr, "Could not allocate buffer of size %d\n", bufferSize);
        return -2;
    }
    int i = 0;
    for (i=0; i < bufferSize; ++i) {
        buffer[i] = 'a' + (i%26);
    }
    buffer[bufferSize] = '\0';

    size_t totalWriteSize = 0;
    for (; totalWriteSize < fileTotalSize; ) {
        tSize toWrite = bufferSize < (fileTotalSize - totalWriteSize) ? bufferSize : (fileTotalSize - totalWriteSize);
        size_t written = hdfsWrite(fs, writeFile, (void*)buffer, toWrite);
        fprintf(stderr, "written size %ld, to write size %d\n", written, toWrite);
        totalWriteSize += written;
        //sleep(1);
    }
    
    free(buffer);
    hdfsCloseFile(fs, writeFile);
    
    fprintf(stderr, "file total size: %lld, total write size: %ld\n", fileTotalSize, totalWriteSize);
    
    hdfsFile readFile = hdfsOpenFile(fs, writeFileName, O_RDONLY, 0, 0, 0);
    //sleep(1);
    fprintf(stderr, "hdfsAvailable: %d\n", hdfsAvailable(fs, readFile));
    
    hdfsFile writeFile2 = hdfsOpenFile(fs, writeFileName, O_WRONLY | O_APPEND, 0, 2, 0);
    fprintf(stderr, "Opened %s for writing successfully...\n", writeFileName);
    const char *content = "Hello, World!";
    size_t num_written_bytes = hdfsWrite(fs, writeFile2, content, strlen(content) + 1);
    if (num_written_bytes != strlen(content) + 1) {
        fprintf(stderr, "Failed to write correct number of bytes - expected %d, got %d\n",
                                    (int)(strlen(content) + 1), (int)num_written_bytes);
        exit(-1);
    }
    fprintf(stderr, "Wrote %zd bytes\n", num_written_bytes);
    
    hdfsDisconnect(fs);
    
    return 0;
}

/**
 * vim: ts=4: sw=4: et:
 */

