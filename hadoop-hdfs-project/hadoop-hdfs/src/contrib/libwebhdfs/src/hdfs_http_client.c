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
#include <pthread.h>
#include "hdfs_http_client.h"

static pthread_mutex_t curlInitMutex = PTHREAD_MUTEX_INITIALIZER;
static volatile int curlGlobalInited = 0;

ResponseBuffer initResponseBuffer() {
    ResponseBuffer info = (ResponseBuffer) calloc(1, sizeof(ResponseBufferInternal));
    if (!info) {
        fprintf(stderr, "Cannot allocate memory for responseInfo\n");
        return NULL;
    }
    info->remaining = 0;
    info->offset = 0;
    info->content = NULL;
    return info;
}

void freeResponseBuffer(ResponseBuffer buffer) {
    if (buffer) {
        if (buffer->content) {
            free(buffer->content);
        }
        free(buffer);
        buffer = NULL;
    }
}

void freeResponse(Response resp)  {
    if(resp) {
        freeResponseBuffer(resp->body);
        freeResponseBuffer(resp->header);
        free(resp);
        resp = NULL;
    }
}

/* Callback for allocating local buffer and reading data to local buffer */
static size_t writefunc(void *ptr, size_t size, size_t nmemb, ResponseBuffer rbuffer) {
    if (size * nmemb < 1) {
        return 0;
    }
    if (!rbuffer) {
        fprintf(stderr, "In writefunc, ResponseBuffer is NULL.\n");
        return -1;
    }
    
    if (rbuffer->remaining < size * nmemb) {
        rbuffer->content = realloc(rbuffer->content, rbuffer->offset + size * nmemb + 1);
        if (rbuffer->content == NULL) {
            return -1;
        }
        rbuffer->remaining = size * nmemb;
    }
    memcpy(rbuffer->content + rbuffer->offset, ptr, size * nmemb);
    rbuffer->offset += size * nmemb;
    (rbuffer->content)[rbuffer->offset] = '\0';
    rbuffer->remaining -= size * nmemb;
    return size * nmemb;
}

/**
 * Callback for reading data to buffer provided by user, 
 * thus no need to reallocate buffer.
 */
static size_t writefunc_withbuffer(void *ptr, size_t size, size_t nmemb, ResponseBuffer rbuffer) {
    if (size * nmemb < 1) {
        return 0;
    }
    if (!rbuffer || !rbuffer->content) {
        fprintf(stderr, "In writefunc_withbuffer, the buffer provided by user is NULL.\n");
        return 0;
    }
    
    size_t toCopy = rbuffer->remaining < (size * nmemb) ? rbuffer->remaining : (size * nmemb);
    memcpy(rbuffer->content + rbuffer->offset, ptr, toCopy);
    rbuffer->offset += toCopy;
    rbuffer->remaining -= toCopy;
    return toCopy;
}

//callback for writing data to remote peer
static size_t readfunc(void *ptr, size_t size, size_t nmemb, void *stream) {
    if (size * nmemb < 1) {
        fprintf(stderr, "In readfunc callback: size * nmemb == %ld\n", size * nmemb);
        return 0;
    }
    webhdfsBuffer *wbuffer = (webhdfsBuffer *) stream;
    
    pthread_mutex_lock(&wbuffer->writeMutex);
    while (wbuffer->remaining == 0) {
        /*
         * the current remainning bytes to write is 0,
         * check whether need to finish the transfer
         * if yes, return 0; else, wait
         */
        if (wbuffer->closeFlag) {
            //we can close the transfer now
            fprintf(stderr, "CloseFlag is set, ready to close the transfer\n");
            pthread_mutex_unlock(&wbuffer->writeMutex);
            return 0;
        } else {
            // len == 0 indicates that user's buffer has been transferred
            pthread_cond_signal(&wbuffer->transfer_finish);
            pthread_cond_wait(&wbuffer->newwrite_or_close, &wbuffer->writeMutex);
        }
    }
    
    if(wbuffer->remaining > 0 && !wbuffer->closeFlag) {
        size_t copySize = wbuffer->remaining < size * nmemb ? wbuffer->remaining : size * nmemb;
        memcpy(ptr, wbuffer->wbuffer + wbuffer->offset, copySize);
        wbuffer->offset += copySize;
        wbuffer->remaining -= copySize;
        pthread_mutex_unlock(&wbuffer->writeMutex);
        return copySize;
    } else {
        fprintf(stderr, "Webhdfs buffer is %ld, it should be a positive value!\n", wbuffer->remaining);
        pthread_mutex_unlock(&wbuffer->writeMutex);
        return 0;
    }
}

static void initCurlGlobal() {
    if (!curlGlobalInited) {
        pthread_mutex_lock(&curlInitMutex);
        if (!curlGlobalInited) {
            curl_global_init(CURL_GLOBAL_ALL);
            curlGlobalInited = 1;
        }
        pthread_mutex_unlock(&curlInitMutex);
    }
}

static Response launchCmd(char *url, enum HttpHeader method, enum Redirect followloc) {
    CURL *curl;
    CURLcode res;
    Response resp;
    
    resp = (Response) calloc(1, sizeof(*resp));
    if (!resp) {
        return NULL;
    }
    resp->body = initResponseBuffer();
    resp->header = initResponseBuffer();
    initCurlGlobal();
    curl = curl_easy_init();                     /* get a curl handle */
    if(curl) {
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writefunc);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, resp->body);
        curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, writefunc);
        curl_easy_setopt(curl, CURLOPT_WRITEHEADER, resp->header);
        curl_easy_setopt(curl, CURLOPT_URL, url);       /* specify target URL */
        switch(method) {
            case GET:
                break;
            case PUT:
                curl_easy_setopt(curl,CURLOPT_CUSTOMREQUEST,"PUT");
                break;
            case POST:
                curl_easy_setopt(curl,CURLOPT_CUSTOMREQUEST,"POST");
                break;
            case DELETE:
                curl_easy_setopt(curl,CURLOPT_CUSTOMREQUEST,"DELETE");
                break;
            default:
                fprintf(stderr, "\nHTTP method not defined\n");
                exit(EXIT_FAILURE);
        }
        if(followloc == YES) {
            curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1);
        }
        
        res = curl_easy_perform(curl);                 /* Now run the curl handler */
        if(res != CURLE_OK) {
            fprintf(stderr, "preform the URL %s failed\n", url);
            return NULL;
        }
        curl_easy_cleanup(curl);
    }
    return resp;
}

static Response launchRead_internal(char *url, enum HttpHeader method, enum Redirect followloc, Response resp) {
    if (!resp || !resp->body || !resp->body->content) {
        fprintf(stderr, "The user provided buffer should not be NULL!\n");
        return NULL;
    }
    
    CURL *curl;
    CURLcode res;
    initCurlGlobal();
    curl = curl_easy_init();                     /* get a curl handle */
    if(curl) {
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writefunc_withbuffer);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, resp->body);
        curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, writefunc);
        curl_easy_setopt(curl, CURLOPT_WRITEHEADER, resp->header);
        curl_easy_setopt(curl, CURLOPT_URL, url);       /* specify target URL */
        if(followloc == YES) {
            curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1);
        }
        
        res = curl_easy_perform(curl);                 /* Now run the curl handler */
        if(res != CURLE_OK && res != CURLE_PARTIAL_FILE) {
            fprintf(stderr, "preform the URL %s failed\n", url);
            return NULL;
        }
        curl_easy_cleanup(curl);
    }
    return resp;

}

static Response launchWrite(const char *url, enum HttpHeader method, webhdfsBuffer *uploadBuffer) {
    if (!uploadBuffer) {
        fprintf(stderr, "upload buffer is NULL!\n");
        errno = EINVAL;
        return NULL;
    }
    initCurlGlobal();
    CURLcode res;
    Response response = (Response) calloc(1, sizeof(*response));
    if (!response) {
        fprintf(stderr, "failed to allocate memory for response\n");
        return NULL;
    }
    response->body = initResponseBuffer();
    response->header = initResponseBuffer();
    
    //connect to the datanode in order to create the lease in the namenode
    CURL *curl = curl_easy_init();
    if (!curl) {
        fprintf(stderr, "Failed to initialize the curl handle.\n");
        return NULL;
    }
    curl_easy_setopt(curl, CURLOPT_URL, url);
    
    if(curl) {
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writefunc);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, response->body);
        curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, writefunc);
        curl_easy_setopt(curl, CURLOPT_WRITEHEADER, response->header);
        curl_easy_setopt(curl, CURLOPT_READFUNCTION, readfunc);
        curl_easy_setopt(curl, CURLOPT_READDATA, uploadBuffer);
        curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
        curl_easy_setopt(curl, CURLOPT_VERBOSE, 1);
        
        struct curl_slist *chunk = NULL;
        chunk = curl_slist_append(chunk, "Transfer-Encoding: chunked");
        res = curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
        chunk = curl_slist_append(chunk, "Expect:");
        res = curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
        
        switch(method) {
            case GET:
                break;
            case PUT:
                curl_easy_setopt(curl,CURLOPT_CUSTOMREQUEST,"PUT");
                break;
            case POST:
                curl_easy_setopt(curl,CURLOPT_CUSTOMREQUEST,"POST");
                break;
            case DELETE:
                curl_easy_setopt(curl,CURLOPT_CUSTOMREQUEST,"DELETE");
                break;
            default:
                fprintf(stderr, "\nHTTP method not defined\n");
                exit(EXIT_FAILURE);
        }
        res = curl_easy_perform(curl);
        curl_slist_free_all(chunk);
        curl_easy_cleanup(curl);
    }
    
    return response;
}

Response launchMKDIR(char *url) {
    return launchCmd(url, PUT, NO);
}

Response launchRENAME(char *url) {
    return launchCmd(url, PUT, NO);
}

Response launchGFS(char *url) {
    return launchCmd(url, GET, NO);
}

Response launchLS(char *url) {
    return launchCmd(url, GET, NO);
}

Response launchCHMOD(char *url) {
    return launchCmd(url, PUT, NO);
}

Response launchCHOWN(char *url) {
    return launchCmd(url, PUT, NO);
}

Response launchDELETE(char *url) {
    return launchCmd(url, DELETE, NO);
}

Response launchOPEN(char *url, Response resp) {
    return launchRead_internal(url, GET, YES, resp);
}

Response launchUTIMES(char *url) {
    return launchCmd(url, PUT, NO);
}

Response launchNnWRITE(char *url) {
    return launchCmd(url, PUT, NO);
}

Response launchNnAPPEND(char *url) {
    return launchCmd(url, POST, NO);
}

Response launchDnWRITE(const char *url, webhdfsBuffer *buffer) {
    return launchWrite(url, PUT, buffer);
}

Response launchDnAPPEND(const char *url, webhdfsBuffer *buffer) {
    return launchWrite(url, POST, buffer);
}

Response launchSETREPLICATION(char *url) {
    return launchCmd(url, PUT, NO);
}
