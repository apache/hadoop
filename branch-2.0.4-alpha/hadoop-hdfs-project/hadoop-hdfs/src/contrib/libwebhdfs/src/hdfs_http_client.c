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

#include <stdlib.h>
#include <string.h>
#include <curl/curl.h>

#include "hdfs_http_client.h"

static pthread_mutex_t curlInitMutex = PTHREAD_MUTEX_INITIALIZER;
static volatile int curlGlobalInited = 0;

const char *hdfs_strerror(int errnoval)
{
    const char *msg = NULL;
    if (errnoval < 0 || errnoval >= sys_nerr) {
        msg = "Invalid Error Code";
    } else if (sys_errlist == NULL) {
        msg = "Unknown Error";
    } else {
        msg = sys_errlist[errnoval];
    }
    return msg;
}

int initResponseBuffer(struct ResponseBuffer **buffer)
{
    struct ResponseBuffer *info = NULL;
    int ret = 0;
    info = calloc(1, sizeof(struct ResponseBuffer));
    if (!info) {
        ret = ENOMEM;
    }
    *buffer = info;
    return ret;
}

void freeResponseBuffer(struct ResponseBuffer *buffer)
{
    if (buffer) {
        if (buffer->content) {
            free(buffer->content);
        }
        free(buffer);
        buffer = NULL;
    }
}

void freeResponse(struct Response *resp)
{
    if (resp) {
        freeResponseBuffer(resp->body);
        freeResponseBuffer(resp->header);
        free(resp);
        resp = NULL;
    }
}

/** 
 * Callback used by libcurl for allocating local buffer and 
 * reading data to local buffer
 */
static size_t writefunc(void *ptr, size_t size,
                        size_t nmemb, struct ResponseBuffer *rbuffer)
{
    void *temp = NULL;
    if (size * nmemb < 1) {
        return 0;
    }
    if (!rbuffer) {
        fprintf(stderr,
                "ERROR: ResponseBuffer is NULL for the callback writefunc.\n");
        return 0;
    }
    
    if (rbuffer->remaining < size * nmemb) {
        temp = realloc(rbuffer->content, rbuffer->offset + size * nmemb + 1);
        if (temp == NULL) {
            fprintf(stderr, "ERROR: fail to realloc in callback writefunc.\n");
            return 0;
        }
        rbuffer->content = temp;
        rbuffer->remaining = size * nmemb;
    }
    memcpy(rbuffer->content + rbuffer->offset, ptr, size * nmemb);
    rbuffer->offset += size * nmemb;
    (rbuffer->content)[rbuffer->offset] = '\0';
    rbuffer->remaining -= size * nmemb;
    return size * nmemb;
}

/**
 * Callback used by libcurl for reading data into buffer provided by user,
 * thus no need to reallocate buffer.
 */
static size_t writeFuncWithUserBuffer(void *ptr, size_t size,
                                   size_t nmemb, struct ResponseBuffer *rbuffer)
{
    size_t toCopy = 0;
    if (size * nmemb < 1) {
        return 0;
    }
    if (!rbuffer || !rbuffer->content) {
        fprintf(stderr,
                "ERROR: buffer to read is NULL for the "
                "callback writeFuncWithUserBuffer.\n");
        return 0;
    }
    
    toCopy = rbuffer->remaining < (size * nmemb) ?
                            rbuffer->remaining : (size * nmemb);
    memcpy(rbuffer->content + rbuffer->offset, ptr, toCopy);
    rbuffer->offset += toCopy;
    rbuffer->remaining -= toCopy;
    return toCopy;
}

/**
 * Callback used by libcurl for writing data to remote peer
 */
static size_t readfunc(void *ptr, size_t size, size_t nmemb, void *stream)
{
    struct webhdfsBuffer *wbuffer = NULL;
    if (size * nmemb < 1) {
        return 0;
    }
    
    wbuffer = stream;
    pthread_mutex_lock(&wbuffer->writeMutex);
    while (wbuffer->remaining == 0) {
        /*
         * The current remainning bytes to write is 0,
         * check closeFlag to see whether need to finish the transfer.
         * if yes, return 0; else, wait
         */
        if (wbuffer->closeFlag) { // We can close the transfer now
            //For debug
            fprintf(stderr, "CloseFlag is set, ready to close the transfer\n");
            pthread_mutex_unlock(&wbuffer->writeMutex);
            return 0;
        } else {
            // remaining == 0 but closeFlag is not set
            // indicates that user's buffer has been transferred
            pthread_cond_signal(&wbuffer->transfer_finish);
            pthread_cond_wait(&wbuffer->newwrite_or_close,
                                    &wbuffer->writeMutex);
        }
    }
    
    if (wbuffer->remaining > 0 && !wbuffer->closeFlag) {
        size_t copySize = wbuffer->remaining < size * nmemb ?
                                wbuffer->remaining : size * nmemb;
        memcpy(ptr, wbuffer->wbuffer + wbuffer->offset, copySize);
        wbuffer->offset += copySize;
        wbuffer->remaining -= copySize;
        pthread_mutex_unlock(&wbuffer->writeMutex);
        return copySize;
    } else {
        fprintf(stderr, "ERROR: webhdfsBuffer's remaining is %ld, "
                "it should be a positive value!\n", wbuffer->remaining);
        pthread_mutex_unlock(&wbuffer->writeMutex);
        return 0;
    }
}

/**
 * Initialize the global libcurl environment
 */
static void initCurlGlobal()
{
    if (!curlGlobalInited) {
        pthread_mutex_lock(&curlInitMutex);
        if (!curlGlobalInited) {
            curl_global_init(CURL_GLOBAL_ALL);
            curlGlobalInited = 1;
        }
        pthread_mutex_unlock(&curlInitMutex);
    }
}

/**
 * Launch simple commands (commands without file I/O) and return response
 *
 * @param url       Target URL
 * @param method    HTTP method (GET/PUT/POST)
 * @param followloc Whether or not need to set CURLOPT_FOLLOWLOCATION
 * @param response  Response from remote service
 * @return 0 for success and non-zero value to indicate error
 */
static int launchCmd(const char *url, enum HttpHeader method,
                     enum Redirect followloc, struct Response **response)
{
    CURL *curl = NULL;
    CURLcode curlCode;
    int ret = 0;
    struct Response *resp = NULL;
    
    resp = calloc(1, sizeof(struct Response));
    if (!resp) {
        return ENOMEM;
    }
    ret = initResponseBuffer(&(resp->body));
    if (ret) {
        goto done;
    }
    ret = initResponseBuffer(&(resp->header));
    if (ret) {
        goto done;
    }
    initCurlGlobal();
    curl = curl_easy_init();
    if (!curl) {
        ret = ENOMEM;       // curl_easy_init does not return error code,
                            // and most of its errors are caused by malloc()
        fprintf(stderr, "ERROR in curl_easy_init.\n");
        goto done;
    }
    /* Set callback function for reading data from remote service */
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writefunc);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, resp->body);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, writefunc);
    curl_easy_setopt(curl, CURLOPT_WRITEHEADER, resp->header);
    curl_easy_setopt(curl, CURLOPT_URL, url);
    switch(method) {
        case GET:
            break;
        case PUT:
            curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
            break;
        case POST:
            curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "POST");
            break;
        case DELETE:
            curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
            break;
        default:
            ret = EINVAL;
            fprintf(stderr, "ERROR: Invalid HTTP method\n");
            goto done;
    }
    if (followloc == YES) {
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1);
    }
    /* Now run the curl handler */
    curlCode = curl_easy_perform(curl);
    if (curlCode != CURLE_OK) {
        ret = EIO;
        fprintf(stderr, "ERROR: preform the URL %s failed, <%d>: %s\n",
                url, curlCode, curl_easy_strerror(curlCode));
    }
done:
    if (curl != NULL) {
        curl_easy_cleanup(curl);
    }
    if (ret) {
        free(resp);
        resp = NULL;
    }
    *response = resp;
    return ret;
}

/**
 * Launch the read request. The request is sent to the NameNode and then 
 * redirected to corresponding DataNode
 *
 * @param url   The URL for the read request
 * @param resp  The response containing the buffer provided by user
 * @return 0 for success and non-zero value to indicate error
 */
static int launchReadInternal(const char *url, struct Response* resp)
{
    CURL *curl;
    CURLcode curlCode;
    int ret = 0;
    
    if (!resp || !resp->body || !resp->body->content) {
        fprintf(stderr,
                "ERROR: invalid user-provided buffer!\n");
        return EINVAL;
    }
    
    initCurlGlobal();
    /* get a curl handle */
    curl = curl_easy_init();
    if (!curl) {
        fprintf(stderr, "ERROR in curl_easy_init.\n");
        return ENOMEM;
    }
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeFuncWithUserBuffer);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, resp->body);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, writefunc);
    curl_easy_setopt(curl, CURLOPT_WRITEHEADER, resp->header);
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1);
    
    curlCode = curl_easy_perform(curl);
    if (curlCode != CURLE_OK && curlCode != CURLE_PARTIAL_FILE) {
        ret = EIO;
        fprintf(stderr, "ERROR: preform the URL %s failed, <%d>: %s\n",
                url, curlCode, curl_easy_strerror(curlCode));
    }
    
    curl_easy_cleanup(curl);
    return ret;
}

/**
 * The function does the write operation by connecting to a DataNode. 
 * The function keeps the connection with the DataNode until 
 * the closeFlag is set. Whenever the current data has been sent out, 
 * the function blocks waiting for further input from user or close.
 *
 * @param url           URL of the remote DataNode
 * @param method        PUT for create and POST for append
 * @param uploadBuffer  Buffer storing user's data to write
 * @param response      Response from remote service
 * @return 0 for success and non-zero value to indicate error
 */
static int launchWrite(const char *url, enum HttpHeader method,
                       struct webhdfsBuffer *uploadBuffer,
                       struct Response **response)
{
    CURLcode curlCode;
    struct Response* resp = NULL;
    struct curl_slist *chunk = NULL;
    CURL *curl = NULL;
    int ret = 0;
    
    if (!uploadBuffer) {
        fprintf(stderr, "ERROR: upload buffer is NULL!\n");
        return EINVAL;
    }
    
    initCurlGlobal();
    resp = calloc(1, sizeof(struct Response));
    if (!resp) {
        return ENOMEM;
    }
    ret = initResponseBuffer(&(resp->body));
    if (ret) {
        goto done;
    }
    ret = initResponseBuffer(&(resp->header));
    if (ret) {
        goto done;
    }
    
    // Connect to the datanode in order to create the lease in the namenode
    curl = curl_easy_init();
    if (!curl) {
        fprintf(stderr, "ERROR: failed to initialize the curl handle.\n");
        return ENOMEM;
    }
    curl_easy_setopt(curl, CURLOPT_URL, url);
    
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writefunc);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, resp->body);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, writefunc);
    curl_easy_setopt(curl, CURLOPT_WRITEHEADER, resp->header);
    curl_easy_setopt(curl, CURLOPT_READFUNCTION, readfunc);
    curl_easy_setopt(curl, CURLOPT_READDATA, uploadBuffer);
    curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
    
    chunk = curl_slist_append(chunk, "Transfer-Encoding: chunked");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
    chunk = curl_slist_append(chunk, "Expect:");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
    
    switch(method) {
        case PUT:
            curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
            break;
        case POST:
            curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "POST");
            break;
        default:
            ret = EINVAL;
            fprintf(stderr, "ERROR: Invalid HTTP method\n");
            goto done;
    }
    curlCode = curl_easy_perform(curl);
    if (curlCode != CURLE_OK) {
        ret = EIO;
        fprintf(stderr, "ERROR: preform the URL %s failed, <%d>: %s\n",
                url, curlCode, curl_easy_strerror(curlCode));
    }
    
done:
    if (chunk != NULL) {
        curl_slist_free_all(chunk);
    }
    if (curl != NULL) {
        curl_easy_cleanup(curl);
    }
    if (ret) {
        free(resp);
        resp = NULL;
    }
    *response = resp;
    return ret;
}

int launchMKDIR(const char *url, struct Response **resp)
{
    return launchCmd(url, PUT, NO, resp);
}

int launchRENAME(const char *url, struct Response **resp)
{
    return launchCmd(url, PUT, NO, resp);
}

int launchGFS(const char *url, struct Response **resp)
{
    return launchCmd(url, GET, NO, resp);
}

int launchLS(const char *url, struct Response **resp)
{
    return launchCmd(url, GET, NO, resp);
}

int launchCHMOD(const char *url, struct Response **resp)
{
    return launchCmd(url, PUT, NO, resp);
}

int launchCHOWN(const char *url, struct Response **resp)
{
    return launchCmd(url, PUT, NO, resp);
}

int launchDELETE(const char *url, struct Response **resp)
{
    return launchCmd(url, DELETE, NO, resp);
}

int launchOPEN(const char *url, struct Response* resp)
{
    return launchReadInternal(url, resp);
}

int launchUTIMES(const char *url, struct Response **resp)
{
    return launchCmd(url, PUT, NO, resp);
}

int launchNnWRITE(const char *url, struct Response **resp)
{
    return launchCmd(url, PUT, NO, resp);
}

int launchNnAPPEND(const char *url, struct Response **resp)
{
    return launchCmd(url, POST, NO, resp);
}

int launchDnWRITE(const char *url, struct webhdfsBuffer *buffer,
                               struct Response **resp)
{
    return launchWrite(url, PUT, buffer, resp);
}

int launchDnAPPEND(const char *url, struct webhdfsBuffer *buffer,
                                struct Response **resp)
{
    return launchWrite(url, POST, buffer, resp);
}

int launchSETREPLICATION(const char *url, struct Response **resp)
{
    return launchCmd(url, PUT, NO, resp);
}
