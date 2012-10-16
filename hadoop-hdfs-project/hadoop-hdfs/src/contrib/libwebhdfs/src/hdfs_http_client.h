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



#ifndef _HDFS_HTTP_CLIENT_H_
#define _HDFS_HTTP_CLIENT_H_

#include "hdfs.h" /* for tSize */

#include <pthread.h> /* for pthread_t */
#include <unistd.h> /* for size_t */

enum hdfsStreamType
{
    UNINITIALIZED = 0,
    INPUT = 1,
    OUTPUT = 2,
};

/**
 * webhdfsBuffer - used for hold the data for read/write from/to http connection
 */
typedef struct {
    const char *wbuffer;  // The user's buffer for uploading
    size_t remaining;     // Length of content
    size_t offset;        // offset for reading
    int openFlag;         // Check whether the hdfsOpenFile has been called before
    int closeFlag;        // Whether to close the http connection for writing
    pthread_mutex_t writeMutex; // Synchronization between the curl and hdfsWrite threads
    pthread_cond_t newwrite_or_close; // Transferring thread waits for this condition
                                      // when there is no more content for transferring in the buffer
    pthread_cond_t transfer_finish; // Condition used to indicate finishing transferring (one buffer)
} webhdfsBuffer;

struct webhdfsFileHandle {
    char *absPath;
    int bufferSize;
    short replication;
    tSize blockSize;
    char *datanode;
    webhdfsBuffer *uploadBuffer;
    pthread_t connThread;
};

enum HttpHeader {
    GET,
    PUT,
    POST,
    DELETE
};

enum Redirect {
    YES,
    NO
};

typedef struct {
    char *content;
    size_t remaining;
    size_t offset;
} ResponseBufferInternal;
typedef ResponseBufferInternal *ResponseBuffer;

/**
 * The response got through webhdfs
 */
typedef struct {
    ResponseBuffer body;
    ResponseBuffer header;
}* Response;

ResponseBuffer initResponseBuffer();
void freeResponseBuffer(ResponseBuffer buffer);
void freeResponse(Response resp);

Response launchMKDIR(char *url);
Response launchRENAME(char *url);
Response launchCHMOD(char *url);
Response launchGFS(char *url);
Response launchLS(char *url);
Response launchDELETE(char *url);
Response launchCHOWN(char *url);
Response launchOPEN(char *url, Response resp);
Response launchUTIMES(char *url);
Response launchNnWRITE(char *url);

Response launchDnWRITE(const char *url, webhdfsBuffer *buffer);
Response launchNnAPPEND(char *url);
Response launchSETREPLICATION(char *url);
Response launchDnAPPEND(const char *url, webhdfsBuffer *buffer);

#endif //_HDFS_HTTP_CLIENT_H_
