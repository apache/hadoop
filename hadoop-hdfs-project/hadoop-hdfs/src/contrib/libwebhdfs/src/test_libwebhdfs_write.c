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
#include <inttypes.h>

int main(int argc, char **argv) {
    hdfsFS fs;
    const char* writeFileName;
    off_t fileTotalSize;
    long long tmpBufferSize;
    tSize bufferSize = 0, totalWriteSize = 0, toWrite = 0, written = 0;
    hdfsFile writeFile = NULL;
    int append, i = 0;
    char* buffer = NULL;
    
    if (argc != 6) {
        fprintf(stderr, "Usage: test_libwebhdfs_write <filename> <filesize> "
                "<buffersize> <username> <append>\n");
        exit(1);
    }
    
    fs = hdfsConnectAsUser("default", 50070, argv[4]);
    if (!fs) {
        fprintf(stderr, "Oops! Failed to connect to hdfs!\n");
        exit(1);
    }
    
    writeFileName = argv[1];
    fileTotalSize = strtoul(argv[2], NULL, 10);
    tmpBufferSize = strtoul(argv[3], NULL, 10);
    
    // sanity check
    if(fileTotalSize == ULONG_MAX && errno == ERANGE) {
        fprintf(stderr, "invalid file size %s - must be <= %lu\n",
                argv[2], ULONG_MAX);
        exit(1);
    }
    
    // currently libhdfs writes are of tSize which is int32
    if(tmpBufferSize > INT_MAX) {
        fprintf(stderr,
                "invalid buffer size libhdfs API write chunks must be <= %d\n",
                INT_MAX);
        exit(1);
    }
    
    bufferSize = (tSize) tmpBufferSize;
    append = atoi(argv[5]);
    if (!append) {
        writeFile = hdfsOpenFile(fs, writeFileName, O_WRONLY, bufferSize, 2, 0);
    } else {
        writeFile = hdfsOpenFile(fs, writeFileName, O_WRONLY | O_APPEND,
                                 bufferSize, 2, 0);
    }
    if (!writeFile) {
        fprintf(stderr, "Failed to open %s for writing!\n", writeFileName);
        exit(1);
    }
    
    // data to be written to the file
    buffer = malloc(sizeof(char) * bufferSize + 1);
    if(buffer == NULL) {
        fprintf(stderr, "Could not allocate buffer of size %d\n", bufferSize);
        exit(1);
    }
    for (i = 0; i < bufferSize; ++i) {
        buffer[i] = 'a' + (i%26);
    }
    buffer[bufferSize] = '\0';

    // write to the file
    totalWriteSize = 0;
    for (; totalWriteSize < fileTotalSize; ) {
        toWrite = bufferSize < (fileTotalSize - totalWriteSize) ?
                            bufferSize : (fileTotalSize - totalWriteSize);
        written = hdfsWrite(fs, writeFile, (void*)buffer, toWrite);
        fprintf(stderr, "written size %d, to write size %d\n",
                written, toWrite);
        totalWriteSize += written;
    }
    
    // cleanup
    free(buffer);
    hdfsCloseFile(fs, writeFile);
    fprintf(stderr, "file total size: %" PRId64 ", total write size: %d\n",
            fileTotalSize, totalWriteSize);
    hdfsDisconnect(fs);
    
    return 0;
}
