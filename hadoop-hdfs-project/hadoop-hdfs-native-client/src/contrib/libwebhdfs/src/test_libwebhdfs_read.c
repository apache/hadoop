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

#include <stdio.h>
#include <stdlib.h>

int main(int argc, char **argv) {

    const char* rfile;
    tSize fileTotalSize, bufferSize, curSize, totalReadSize;
    hdfsFS fs;
    hdfsFile readFile;
    char *buffer = NULL;
    
    if (argc != 4) {
        fprintf(stderr, "Usage: test_libwebhdfs_read"
                " <filename> <filesize> <buffersize>\n");
        exit(1);
    }
    
    fs = hdfsConnect("localhost", 50070);
    if (!fs) {
        fprintf(stderr, "Oops! Failed to connect to hdfs!\n");
        exit(1);
    }
    
    rfile = argv[1];
    fileTotalSize = strtoul(argv[2], NULL, 10);
    bufferSize = strtoul(argv[3], NULL, 10);
    
    readFile = hdfsOpenFile(fs, rfile, O_RDONLY, bufferSize, 0, 0);
    if (!readFile) {
        fprintf(stderr, "Failed to open %s for writing!\n", rfile);
        exit(1);
    }
    
    // data to be written to the file
    buffer = malloc(sizeof(char) * bufferSize);
    if(buffer == NULL) {
        fprintf(stderr, "Failed to allocate buffer.\n");
        exit(1);
    }
    
    // read from the file
    curSize = bufferSize;
    totalReadSize = 0;
    for (; (curSize = hdfsRead(fs, readFile, buffer, bufferSize)) == bufferSize; ) {
        totalReadSize += curSize;
    }
    totalReadSize += curSize;
    
    fprintf(stderr, "size of the file: %d; reading size: %d\n",
            fileTotalSize, totalReadSize);
    
    free(buffer);
    hdfsCloseFile(fs, readFile);
    hdfsDisconnect(fs);
    
    return 0;
}

