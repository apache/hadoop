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

/** enum indicating the type of hdfs stream */
enum hdfsStreamType
{
    UNINITIALIZED = 0,
    INPUT = 1,
    OUTPUT = 2,
};

/**
 * webhdfsBuffer - used for hold the data for read/write from/to http connection
 */
struct webhdfsBuffer {
    const char *wbuffer;  /* The user's buffer for uploading */
    size_t remaining;     /* Length of content */
    size_t offset;        /* offset for reading */
    /* Check whether the hdfsOpenFile has been called before */
    int openFlag;
    /* Whether to close the http connection for writing */
    int closeFlag;
    /* Synchronization between the curl and hdfsWrite threads */
    pthread_mutex_t writeMutex;
    /* 
     * Transferring thread waits for this condition
     * when there is no more content for transferring in the buffer
     */
    pthread_cond_t newwrite_or_close;
    /* Condition used to indicate finishing transferring (one buffer) */
    pthread_cond_t transfer_finish;
};

/** File handle for webhdfs */
struct webhdfsFileHandle {
    char *absPath;        /* Absolute path of file */
    int bufferSize;       /* Size of buffer */
    short replication;    /* Number of replication */
    tSize blockSize;      /* Block size */
    char *datanode;       /* URL of the DataNode */
    /* webhdfsBuffer handle used to store the upload data */
    struct webhdfsBuffer *uploadBuffer;
    /* The thread used for data transferring */
    pthread_t connThread;
};

/** Type of http header */
enum HttpHeader {
    GET,
    PUT,
    POST,
    DELETE
};

/** Whether to redirect */
enum Redirect {
    YES,
    NO
};

/** Buffer used for holding response */
struct ResponseBuffer {
    char *content;
    size_t remaining;
    size_t offset;
};

/**
 * The response got through webhdfs
 */
struct Response {
    struct ResponseBuffer *body;
    struct ResponseBuffer *header;
};

/**
 * Create and initialize a ResponseBuffer
 *
 * @param buffer Pointer pointing to new created ResponseBuffer handle
 * @return 0 for success, non-zero value to indicate error
 */
int initResponseBuffer(struct ResponseBuffer **buffer) __attribute__ ((warn_unused_result));

/**
 * Free the given ResponseBuffer
 *
 * @param buffer The ResponseBuffer to free
 */
void freeResponseBuffer(struct ResponseBuffer *buffer);

/**
 * Free the given Response
 *
 * @param resp The Response to free
 */
void freeResponse(struct Response *resp);

/**
 * Send the MKDIR request to NameNode using the given URL. 
 * The NameNode will execute the operation and return the result as response.
 *
 * @param url The URL for MKDIR operation
 * @param response Response handle to store response returned from the NameNode
 * @return 0 for success, non-zero value to indicate error
 */
int launchMKDIR(const char *url,
                struct Response **response) __attribute__ ((warn_unused_result));

/**
 * Send the RENAME request to NameNode using the given URL.
 * The NameNode will execute the operation and return the result as response.
 *
 * @param url The URL for RENAME operation
 * @param response Response handle to store response returned from the NameNode
 * @return 0 for success, non-zero value to indicate error
 */
int launchRENAME(const char *url,
                 struct Response **response) __attribute__ ((warn_unused_result));

/**
 * Send the CHMOD request to NameNode using the given URL.
 * The NameNode will execute the operation and return the result as response.
 *
 * @param url The URL for CHMOD operation
 * @param response Response handle to store response returned from the NameNode
 * @return 0 for success, non-zero value to indicate error
 */
int launchCHMOD(const char *url,
                struct Response **response) __attribute__ ((warn_unused_result));

/**
 * Send the GetFileStatus request to NameNode using the given URL.
 * The NameNode will execute the operation and return the result as response.
 *
 * @param url The URL for GetFileStatus operation
 * @param response Response handle to store response returned from the NameNode,
 *                 containing either file status or exception information
 * @return 0 for success, non-zero value to indicate error
 */
int launchGFS(const char *url,
              struct Response **response) __attribute__ ((warn_unused_result));

/**
 * Send the LS (LISTSTATUS) request to NameNode using the given URL.
 * The NameNode will execute the operation and return the result as response.
 *
 * @param url The URL for LISTSTATUS operation
 * @param response Response handle to store response returned from the NameNode
 * @return 0 for success, non-zero value to indicate error
 */
int launchLS(const char *url,
             struct Response **response) __attribute__ ((warn_unused_result));

/**
 * Send the DELETE request to NameNode using the given URL.
 * The NameNode will execute the operation and return the result as response.
 *
 * @param url The URL for DELETE operation
 * @param response Response handle to store response returned from the NameNode
 * @return 0 for success, non-zero value to indicate error
 */
int launchDELETE(const char *url,
                 struct Response **response) __attribute__ ((warn_unused_result));

/**
 * Send the CHOWN request to NameNode using the given URL.
 * The NameNode will execute the operation and return the result as response.
 *
 * @param url The URL for CHOWN operation
 * @param response Response handle to store response returned from the NameNode
 * @return 0 for success, non-zero value to indicate error
 */
int launchCHOWN(const char *url,
                struct Response **response) __attribute__ ((warn_unused_result));

/**
 * Send the OPEN request to NameNode using the given URL, 
 * asking for reading a file (within a range). 
 * The NameNode first redirects the request to the datanode
 * that holds the corresponding first block of the file (within a range),
 * and the datanode returns the content of the file through the HTTP connection.
 *
 * @param url The URL for OPEN operation
 * @param resp The response holding user's buffer. 
               The file content will be written into the buffer.
 * @return 0 for success, non-zero value to indicate error
 */
int launchOPEN(const char *url,
               struct Response* resp) __attribute__ ((warn_unused_result));

/**
 * Send the SETTIMES request to NameNode using the given URL.
 * The NameNode will execute the operation and return the result as response.
 *
 * @param url The URL for SETTIMES operation
 * @param response Response handle to store response returned from the NameNode
 * @return 0 for success, non-zero value to indicate error
 */
int launchUTIMES(const char *url,
                 struct Response **response) __attribute__ ((warn_unused_result));

/**
 * Send the WRITE/CREATE request to NameNode using the given URL.
 * The NameNode will choose the writing target datanodes 
 * and return the first datanode in the pipeline as response
 *
 * @param url The URL for WRITE/CREATE operation connecting to NameNode
 * @param response Response handle to store response returned from the NameNode
 * @return 0 for success, non-zero value to indicate error
 */
int launchNnWRITE(const char *url,
                  struct Response **response) __attribute__ ((warn_unused_result));

/**
 * Send the WRITE request along with to-write content to 
 * the corresponding DataNode using the given URL. 
 * The DataNode will write the data and return the response.
 *
 * @param url The URL for WRITE operation connecting to DataNode
 * @param buffer The webhdfsBuffer containing data to be written to hdfs
 * @param response Response handle to store response returned from the NameNode
 * @return 0 for success, non-zero value to indicate error
 */
int launchDnWRITE(const char *url, struct webhdfsBuffer *buffer,
                  struct Response **response) __attribute__ ((warn_unused_result));

/**
 * Send the WRITE (APPEND) request to NameNode using the given URL. 
 * The NameNode determines the DataNode for appending and 
 * sends its URL back as response.
 *
 * @param url The URL for APPEND operation
 * @param response Response handle to store response returned from the NameNode
 * @return 0 for success, non-zero value to indicate error
 */
int launchNnAPPEND(const char *url, struct Response **response) __attribute__ ((warn_unused_result));

/**
 * Send the SETREPLICATION request to NameNode using the given URL.
 * The NameNode will execute the operation and return the result as response.
 *
 * @param url The URL for SETREPLICATION operation
 * @param response Response handle to store response returned from the NameNode
 * @return 0 for success, non-zero value to indicate error
 */
int launchSETREPLICATION(const char *url,
                         struct Response **response) __attribute__ ((warn_unused_result));

/**
 * Send the APPEND request along with the content to DataNode.
 * The DataNode will do the appending and return the result as response.
 *
 * @param url The URL for APPEND operation connecting to DataNode
 * @param buffer The webhdfsBuffer containing data to be appended
 * @param response Response handle to store response returned from the NameNode
 * @return 0 for success, non-zero value to indicate error
 */
int launchDnAPPEND(const char *url, struct webhdfsBuffer *buffer,
                   struct Response **response) __attribute__ ((warn_unused_result));

/**
 * Call sys_errlist to get the error message string for the given error code
 *
 * @param errnoval  The error code value
 * @return          The error message string mapped to the given error code
 */
const char *hdfs_strerror(int errnoval);

#endif //_HDFS_HTTP_CLIENT_H_
