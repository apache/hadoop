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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <jni.h>
#include "webhdfs.h"
#include "hdfs_http_client.h"
#include "hdfs_http_query.h"
#include "hdfs_json_parser.h"
#include "jni_helper.h"
#include "exception.h"

#define HADOOP_HDFS_CONF        "org/apache/hadoop/hdfs/HdfsConfiguration"
#define HADOOP_NAMENODE         "org/apache/hadoop/hdfs/server/namenode/NameNode"
#define JAVA_INETSOCKETADDRESS  "java/net/InetSocketAddress"

static void initFileinfo(hdfsFileInfo *fileInfo) {
    if (fileInfo) {
        fileInfo->mKind = kObjectKindFile;
        fileInfo->mName = NULL;
        fileInfo->mLastMod = 0;
        fileInfo->mSize = 0;
        fileInfo->mReplication = 0;
        fileInfo->mBlockSize = 0;
        fileInfo->mOwner = NULL;
        fileInfo->mGroup = NULL;
        fileInfo->mPermissions = 0;
        fileInfo->mLastAccess = 0;
    }
}

static webhdfsBuffer *initWebHdfsBuffer() {
    webhdfsBuffer *buffer = (webhdfsBuffer *) calloc(1, sizeof(webhdfsBuffer));
    if (!buffer) {
        fprintf(stderr, "Fail to allocate memory for webhdfsBuffer.\n");
        return NULL;
    }
    buffer->remaining = 0;
    buffer->offset = 0;
    buffer->wbuffer = NULL;
    buffer->closeFlag = 0;
    buffer->openFlag = 0;
    pthread_mutex_init(&buffer->writeMutex, NULL);
    pthread_cond_init(&buffer->newwrite_or_close, NULL);
    pthread_cond_init(&buffer->transfer_finish, NULL);
    return buffer;
}

static webhdfsBuffer *resetWebhdfsBuffer(webhdfsBuffer *wb, const char *buffer, size_t length) {
    if (buffer && length > 0) {
        pthread_mutex_lock(&wb->writeMutex);
        wb->wbuffer = buffer;
        wb->offset = 0;
        wb->remaining = length;
        pthread_cond_signal(&wb->newwrite_or_close);
        while (wb->remaining != 0) {
            pthread_cond_wait(&wb->transfer_finish, &wb->writeMutex);
        }
        pthread_mutex_unlock(&wb->writeMutex);
    }
    return wb;
}

static void freeWebhdfsBuffer(webhdfsBuffer *buffer) {
    if (buffer) {
        int des = pthread_cond_destroy(&buffer->newwrite_or_close);
        if (des == EBUSY) {
            fprintf(stderr, "The condition newwrite_or_close is still referenced!\n");
        } else if (des == EINVAL) {
            fprintf(stderr, "The condition newwrite_or_close is invalid!\n");
        }
        des = pthread_cond_destroy(&buffer->transfer_finish);
        if (des == EBUSY) {
            fprintf(stderr, "The condition transfer_finish is still referenced!\n");
        } else if (des == EINVAL) {
            fprintf(stderr, "The condition transfer_finish is invalid!\n");
        }
        if (des == EBUSY) {
            fprintf(stderr, "The condition close_clean is still referenced!\n");
        } else if (des == EINVAL) {
            fprintf(stderr, "The condition close_clean is invalid!\n");
        }
        des = pthread_mutex_destroy(&buffer->writeMutex);
        if (des == EBUSY) {
            fprintf(stderr, "The mutex is still locked or referenced!\n");
        }
        free(buffer);
        buffer = NULL;
    }
}

static void freeWebFileHandle(struct webhdfsFileHandle * handle) {
    if (handle) {
        freeWebhdfsBuffer(handle->uploadBuffer);
        if (handle->datanode) {
            free(handle->datanode);
        }
        if (handle->absPath) {
            free(handle->absPath);
        }
        free(handle);
        handle = NULL;
    }
}

struct hdfsBuilder *hdfsNewBuilder(void)
{
    struct hdfsBuilder *bld = calloc(1, sizeof(struct hdfsBuilder));
    if (!bld) {
        return NULL;
    }
    hdfsSetWorkingDirectory(bld, "/");
    return bld;
}

void hdfsFreeBuilder(struct hdfsBuilder *bld)
{
    if (bld && bld->workingDir) {
        free(bld->workingDir);
    }
    free(bld);
}

void hdfsBuilderSetForceNewInstance(struct hdfsBuilder *bld)
{
    if (bld) {
        bld->forceNewInstance = 1;
    }
}

void hdfsBuilderSetNameNode(struct hdfsBuilder *bld, const char *nn)
{
    if (bld) {
        bld->nn = nn;
        bld->nn_jni = nn;
    }
}

void hdfsBuilderSetNameNodePort(struct hdfsBuilder *bld, tPort port)
{
    if (bld) {
        bld->port = port;
    }
}

void hdfsBuilderSetUserName(struct hdfsBuilder *bld, const char *userName)
{
    if (bld) {
        bld->userName = userName;
    }
}

void hdfsBuilderSetKerbTicketCachePath(struct hdfsBuilder *bld,
                                       const char *kerbTicketCachePath)
{
    if (bld) {
        bld->kerbTicketCachePath = kerbTicketCachePath;
    }
}

hdfsFS hdfsConnectAsUser(const char* nn, tPort port, const char *user)
{
    struct hdfsBuilder* bld = hdfsNewBuilder();
    if (!bld) {
        return NULL;
    }
    hdfsBuilderSetNameNode(bld, nn);
    hdfsBuilderSetNameNodePort(bld, port);
    hdfsBuilderSetUserName(bld, user);
    return hdfsBuilderConnect(bld);
}

hdfsFS hdfsConnect(const char* nn, tPort port)
{
    return hdfsConnectAsUser(nn, port, NULL);
}

hdfsFS hdfsConnectNewInstance(const char* nn, tPort port)
{
    struct hdfsBuilder* bld = (struct hdfsBuilder *) hdfsConnect(nn, port);
    if (!bld) {
        return NULL;
    }
    hdfsBuilderSetForceNewInstance(bld);
    return bld;
}

hdfsFS hdfsConnectAsUserNewInstance(const char* host, tPort port,
                                    const char *user)
{
    struct hdfsBuilder *bld = hdfsNewBuilder();
    if (!bld)
        return NULL;
    hdfsBuilderSetNameNode(bld, host);
    hdfsBuilderSetNameNodePort(bld, port);
    hdfsBuilderSetUserName(bld, user);
    hdfsBuilderSetForceNewInstance(bld);
    return hdfsBuilderConnect(bld);
}

const char *hdfsBuilderToStr(const struct hdfsBuilder *bld,
                             char *buf, size_t bufLen);

hdfsFS hdfsBuilderConnect(struct hdfsBuilder *bld)
{
    if (!bld) {
        return NULL;
    }
    // if the hostname is null for the namenode, set it to localhost
    //only handle bld->nn
    if (bld->nn == NULL) {
        bld->nn = "localhost";
    } else {
        /* check whether the hostname of the namenode (nn in hdfsBuilder) has already contained the port */
        const char *lastColon = rindex(bld->nn, ':');
        if (lastColon && (strspn(lastColon + 1, "0123456789") == strlen(lastColon + 1))) {
            fprintf(stderr, "port %d was given, but URI '%s' already "
                    "contains a port!\n", bld->port, bld->nn);
            char *newAddr = (char *)malloc(strlen(bld->nn) - strlen(lastColon) + 1);
            if (!newAddr) {
                return NULL;
            }
            strncpy(newAddr, bld->nn, strlen(bld->nn) - strlen(lastColon));
            newAddr[strlen(bld->nn) - strlen(lastColon)] = '\0';
            free(bld->nn);
            bld->nn = newAddr;
        }
    }
    
    /* if the namenode is "default" and/or the port of namenode is 0, get the default namenode/port by using JNI */
    if (bld->port == 0 || !strcasecmp("default", bld->nn)) {
        JNIEnv *env = 0;
        jobject jHDFSConf = NULL, jAddress = NULL;
        jvalue jVal;
        jthrowable jthr = NULL;
        int ret = 0;
        char buf[512];
        
        //Get the JNIEnv* corresponding to current thread
        env = getJNIEnv();
        if (env == NULL) {
            errno = EINTERNAL;
            free(bld);
            bld = NULL;
            return NULL;
        }
        
        //  jHDFSConf = new HDFSConfiguration();
        jthr = constructNewObjectOfClass(env, &jHDFSConf, HADOOP_HDFS_CONF, "()V");
        if (jthr) {
            ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                                        "hdfsBuilderConnect(%s)",
                                        hdfsBuilderToStr(bld, buf, sizeof(buf)));
            goto done;
        }
        
        jthr = invokeMethod(env, &jVal, STATIC, NULL, HADOOP_NAMENODE, "getHttpAddress",
                            "(Lorg/apache/hadoop/conf/Configuration;)Ljava/net/InetSocketAddress;",
                            jHDFSConf);
        if (jthr) {
            ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                            "hdfsBuilderConnect(%s)", hdfsBuilderToStr(bld, buf, sizeof(buf)));
            goto done; //free(bld), deleteReference for jHDFSConf
        }
        jAddress = jVal.l;
        
        if (bld->port == 0) {
            jthr = invokeMethod(env, &jVal, INSTANCE, jAddress,
                                JAVA_INETSOCKETADDRESS, "getPort", "()I");
            if (jthr) {
                ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                                            "hdfsBuilderConnect(%s)",
                                            hdfsBuilderToStr(bld, buf, sizeof(buf)));
                goto done;
            }
            bld->port = jVal.i;
        }
        
        if (!strcasecmp("default", bld->nn)) {
            jthr = invokeMethod(env, &jVal, INSTANCE, jAddress,
                                JAVA_INETSOCKETADDRESS, "getHostName", "()Ljava/lang/String;");
            if (jthr) {
                ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                                            "hdfsBuilderConnect(%s)",
                                            hdfsBuilderToStr(bld, buf, sizeof(buf)));
                goto done;
            }
            bld->nn = (const char*) ((*env)->GetStringUTFChars(env, jVal.l, NULL));
        }
        
    done:
        destroyLocalReference(env, jHDFSConf);
        destroyLocalReference(env, jAddress);
        if (ret) { //if there is error/exception, we free the builder and return NULL
            free(bld);
            bld = NULL;
        }
    }
    
    //for debug
    fprintf(stderr, "namenode: %s:%d\n", bld->nn, bld->port);
    return bld;
}

int hdfsDisconnect(hdfsFS fs)
{
    if (fs == NULL) {
        errno = EBADF;
        return -1;
    } else {
        free(fs);
        fs = NULL;
    }
    return 0;
}

char *getAbsolutePath(hdfsFS fs, const char *path) {
    if (fs == NULL || path == NULL) {
        return NULL;
    }
    char *absPath = NULL;
    struct hdfsBuilder *bld = (struct hdfsBuilder *) fs;
    
    if ('/' != *path && bld->workingDir) {
        absPath = (char *)malloc(strlen(bld->workingDir) + strlen(path) + 1);
        if (!absPath) {
            return NULL;
        }
        absPath = strcpy(absPath, bld->workingDir);
        absPath = strcat(absPath, path);
        return absPath;
    } else {
        absPath = (char *)malloc(strlen(path) + 1);
        if (!absPath) {
            return NULL;
        }
        absPath = strcpy(absPath, path);
        return absPath;
    }
}

int hdfsCreateDirectory(hdfsFS fs, const char* path)
{
    if (fs == NULL || path == NULL) {
        return -1;
    }
    
    char *absPath = getAbsolutePath(fs, path);
    if (!absPath) {
        return -1;
    }
    
    struct hdfsBuilder *bld = (struct hdfsBuilder *) fs;
    char *url = NULL;
    Response resp = NULL;
    int ret = 0;
    
    if(!((url = prepareMKDIR(bld->nn, bld->port, absPath, bld->userName))
         && (resp = launchMKDIR(url))
         && (parseMKDIR(resp->body->content)))) {
        ret = -1;
    }
    
    freeResponse(resp);
    free(url);
    free(absPath);
    return ret;
}

int hdfsChmod(hdfsFS fs, const char* path, short mode)
{
    if (fs == NULL || path == NULL) {
        return -1;
    }
    
    char *absPath = getAbsolutePath(fs, path);
    if (!absPath) {
        return -1;
    }
    struct hdfsBuilder *bld = (struct hdfsBuilder *) fs;
    char *url=NULL;
    Response resp = NULL;
    int ret = 0;
    
    if(!((url = prepareCHMOD(bld->nn, bld->port, absPath, (int)mode, bld->userName))
         && (resp = launchCHMOD(url))
         && (parseCHMOD(resp->header->content, resp->body->content)))) {
        ret = -1;
    }
    
    freeResponse(resp);
    free(absPath);
    free(url);
    return ret;
}

int hdfsChown(hdfsFS fs, const char* path, const char *owner, const char *group)
{
    if (fs == NULL || path == NULL) {
        return -1;
    }
    
    char *absPath = getAbsolutePath(fs, path);
    if (!absPath) {
        return -1;
    }
    struct hdfsBuilder *bld = (struct hdfsBuilder *) fs;
    char *url=NULL;
    Response resp = NULL;
    int ret = 0;
    
    if(!((url = prepareCHOWN(bld->nn, bld->port, absPath, owner, group, bld->userName))
         && (resp = launchCHOWN(url))
         && (parseCHOWN(resp->header->content, resp->body->content)))) {
        ret = -1;
    }
    
    freeResponse(resp);
    free(absPath);
    free(url);
    return ret;
}

int hdfsRename(hdfsFS fs, const char* oldPath, const char* newPath)
{
    if (fs == NULL || oldPath == NULL || newPath == NULL) {
        return -1;
    }
    
    char *oldAbsPath = getAbsolutePath(fs, oldPath);
    if (!oldAbsPath) {
        return -1;
    }
    char *newAbsPath = getAbsolutePath(fs, newPath);
    if (!newAbsPath) {
        return -1;
    }
    
    struct hdfsBuilder *bld = (struct hdfsBuilder *) fs;
    char *url=NULL;
    Response resp = NULL;
    int ret = 0;
    
    if(!((url = prepareRENAME(bld->nn, bld->port, oldAbsPath, newAbsPath, bld->userName))
         && (resp = launchRENAME(url))
         && (parseRENAME(resp->body->content)))) {
        ret = -1;
    }
    
    freeResponse(resp);
    free(oldAbsPath);
    free(newAbsPath);
    free(url);
    return ret;
}

hdfsFileInfo *hdfsGetPathInfo(hdfsFS fs, const char* path)
{
    if (fs == NULL || path == NULL) {
        return NULL;
    }
    
    char *absPath = getAbsolutePath(fs, path);
    if (!absPath) {
        return NULL;
    }
    
    struct hdfsBuilder *bld = (struct hdfsBuilder *) fs;
    char *url=NULL;
    Response resp = NULL;
    int numEntries = 0;
    int ret = 0;
    
    hdfsFileInfo * fileInfo = (hdfsFileInfo *) calloc(1, sizeof(hdfsFileInfo));
    if (!fileInfo) {
        ret = -1;
        goto done;
    }
    initFileinfo(fileInfo);
    
    if(!((url = prepareGFS(bld->nn, bld->port, absPath, bld->userName))
         && (resp = launchGFS(url))
         && (fileInfo = parseGFS(resp->body->content, fileInfo, &numEntries))))  {
        ret = -1;
        goto done;
    }
    
done:
    freeResponse(resp);
    free(absPath);
    free(url);
    
    if (ret == 0) {
        return fileInfo;
    } else {
        free(fileInfo);
        return NULL;
    }
}

hdfsFileInfo *hdfsListDirectory(hdfsFS fs, const char* path, int *numEntries)
{
    if (fs == NULL || path == NULL) {
        return NULL;
    }
    
    char *absPath = getAbsolutePath(fs, path);
    if (!absPath) {
        return NULL;
    }

    struct hdfsBuilder *bld = (struct hdfsBuilder *) fs;
    char *url = NULL;
    Response resp = NULL;
    int ret = 0;
    
    hdfsFileInfo * fileInfo = (hdfsFileInfo *) calloc(1, sizeof(hdfsFileInfo));
    if (!fileInfo) {
        ret = -1;
        goto done;
    }
    
    if(!((url = prepareLS(bld->nn, bld->port, absPath, bld->userName))
         && (resp = launchLS(url))
         && (fileInfo = parseGFS(resp->body->content, fileInfo, numEntries))))  {
        ret = -1;
        goto done;
    }
    
done:
    freeResponse(resp);
    free(absPath);
    free(url);
    
    if (ret == 0) {
        return fileInfo;
    } else {
        free(fileInfo);
        return NULL;
    }
}

int hdfsSetReplication(hdfsFS fs, const char* path, int16_t replication)
{
    if (fs == NULL || path == NULL) {
        return -1;
    }
    char *absPath = getAbsolutePath(fs, path);
    if (!absPath) {
        return -1;
    }
    
    struct hdfsBuilder *bld = (struct hdfsBuilder *) fs;
    char *url = NULL;
    Response resp = NULL;
    int ret = 0;
    
    if(!((url = prepareSETREPLICATION(bld->nn, bld->port, absPath, replication, bld->userName))
         && (resp = launchSETREPLICATION(url))
         && (parseSETREPLICATION(resp->body->content)))) {
        ret = -1;
    }
    
    freeResponse(resp);
    free(absPath);
    free(url);
    return ret;
}

void hdfsFreeFileInfo(hdfsFileInfo *hdfsFileInfo, int numEntries)
{
    //Free the mName, mOwner, and mGroup
    int i;
    for (i=0; i < numEntries; ++i) {
        if (hdfsFileInfo[i].mName) {
            free(hdfsFileInfo[i].mName);
        }
        if (hdfsFileInfo[i].mOwner) {
            free(hdfsFileInfo[i].mOwner);
        }
        if (hdfsFileInfo[i].mGroup) {
            free(hdfsFileInfo[i].mGroup);
        }
    }
    
    //Free entire block
    free(hdfsFileInfo);
    hdfsFileInfo = NULL;
}

int hdfsDelete(hdfsFS fs, const char* path, int recursive)
{
    if (fs == NULL || path == NULL) {
        return -1;
    }
    char *absPath = getAbsolutePath(fs, path);
    if (!absPath) {
        return -1;
    }
    
    struct hdfsBuilder *bld = (struct hdfsBuilder *) fs;
    char *url = NULL;
    Response resp = NULL;
    int ret = 0;
    
    if(!((url = prepareDELETE(bld->nn, bld->port, absPath, recursive, bld->userName))
         && (resp = launchDELETE(url))
         && (parseDELETE(resp->body->content)))) {
        ret = -1;
    }
    
    freeResponse(resp);
    free(absPath);
    free(url);
    return ret;
}

int hdfsUtime(hdfsFS fs, const char* path, tTime mtime, tTime atime)
{
    if (fs == NULL || path == NULL) {
        return -1;
    }
    char *absPath = getAbsolutePath(fs, path);
    if (!absPath) {
        return -1;
    }
    
    struct hdfsBuilder *bld = (struct hdfsBuilder *) fs;
    char *url = NULL;
    Response resp = NULL;
    int ret = 0;
    
    if(!((url = prepareUTIMES(bld->nn, bld->port, absPath, mtime, atime, bld->userName))
         && (resp = launchUTIMES(url))
         && (parseUTIMES(resp->header->content, resp->body->content)))) {
        ret = -1;
    }
    
    freeResponse(resp);
    free(absPath);
    free(url);
    return ret;
}

int hdfsExists(hdfsFS fs, const char *path)
{
    hdfsFileInfo *fileInfo = hdfsGetPathInfo(fs, path);
    if (fileInfo) {
        hdfsFreeFileInfo(fileInfo, 1);
        return 0;
    } else {
        return -1;
    }
}

typedef struct {
    char *url;
    webhdfsBuffer *uploadBuffer;
    int flags;
    Response resp;
} threadData;

static void freeThreadData(threadData *data) {
    if (data) {
        if (data->url) {
            free(data->url);
        }
        if (data->resp) {
            freeResponse(data->resp);
        }
        //the uploadBuffer would be freed by freeWebFileHandle()
        free(data);
        data = NULL;
    }
}

static void *writeThreadOperation(void *v) {
    threadData *data = (threadData *) v;
    if (data->flags & O_APPEND) {
        data->resp = launchDnAPPEND(data->url, data->uploadBuffer);
    } else {
        data->resp = launchDnWRITE(data->url, data->uploadBuffer);
    }
    return data;
}

hdfsFile hdfsOpenFile(hdfsFS fs, const char* path, int flags,
                      int bufferSize, short replication, tSize blockSize)
{
    /*
     * the original version of libhdfs based on JNI store a fsinputstream/fsoutputstream in the hdfsFile
     * in libwebhdfs that is based on webhdfs, we store (absolute_path, buffersize, replication, blocksize) in it
     */
    if (fs == NULL || path == NULL) {
        return NULL;
    }

    int accmode = flags & O_ACCMODE;
    if (accmode == O_RDWR) {
        fprintf(stderr, "ERROR: cannot open an hdfs file in O_RDWR mode\n");
        errno = ENOTSUP;
        return NULL;
    }
    
    if ((flags & O_CREAT) && (flags & O_EXCL)) {
        fprintf(stderr, "WARN: hdfs does not truly support O_CREATE && O_EXCL\n");
    }
    
    hdfsFile hdfsFileHandle = (hdfsFile) calloc(1, sizeof(struct hdfsFile_internal));
    if (!hdfsFileHandle) {
        return NULL;
    }
    int ret = 0;
    hdfsFileHandle->flags = flags;
    hdfsFileHandle->type = accmode == O_RDONLY ? INPUT : OUTPUT;
    hdfsFileHandle->offset = 0;
    struct webhdfsFileHandle *webhandle = (struct webhdfsFileHandle *) calloc(1, sizeof(struct webhdfsFileHandle));
    if (!webhandle) {
        ret = -1;
        goto done;
    }
    webhandle->bufferSize = bufferSize;
    webhandle->replication = replication;
    webhandle->blockSize = blockSize;
    webhandle->absPath = getAbsolutePath(fs, path);
    if (!webhandle->absPath) {
        ret = -1;
        goto done;
    }
    hdfsFileHandle->file = webhandle;
    
    //for write/append, need to connect to the namenode
    //and get the url of corresponding datanode
    if (hdfsFileHandle->type == OUTPUT) {
        webhandle->uploadBuffer = initWebHdfsBuffer();
        if (!webhandle->uploadBuffer) {
            ret = -1;
            goto done;
        }
        struct hdfsBuilder *bld = (struct hdfsBuilder *) fs;
        char *url = NULL;
        Response resp = NULL;
        int append = flags & O_APPEND;
        int create = append ? 0 : 1;
        
        //if create: send create request to NN
        if (create) {
            url = prepareNnWRITE(bld->nn, bld->port, webhandle->absPath, bld->userName, webhandle->replication, webhandle->blockSize);
        } else if (append) {
            url = prepareNnAPPEND(bld->nn, bld->port, webhandle->absPath, bld->userName);
        }
        if (!url) {
            fprintf(stderr,
                    "fail to create the url connecting to namenode for file creation/appending\n");
            ret = -1;
            goto done;
        }

        if (create) {
            resp = launchNnWRITE(url);
        } else if (append) {
            resp = launchNnAPPEND(url);
        }
        if (!resp) {
            fprintf(stderr,
                    "fail to get the response from namenode for file creation/appending\n");
            free(url);
            ret = -1;
            goto done;
        }
        
        int parseRet = 0;
        if (create) {
            parseRet = parseNnWRITE(resp->header->content, resp->body->content);
        } else if (append) {
            parseRet = parseNnAPPEND(resp->header->content, resp->body->content);
        }
        if (!parseRet) {
            fprintf(stderr,
                    "fail to parse the response from namenode for file creation/appending\n");
            free(url);
            freeResponse(resp);
            ret = -1;
            goto done;
        }
            
        free(url);
        url = parseDnLoc(resp->header->content);
        if (!url) {
            fprintf(stderr,
                    "fail to get the datanode url from namenode for file creation/appending\n");
            freeResponse(resp);
            ret = -1;
            return NULL;
        }
        freeResponse(resp);
        //store the datanode url in the file handle
        webhandle->datanode = strdup(url);
 
        //create a new thread for performing the http transferring
        threadData *data = (threadData *) calloc(1, sizeof(threadData));
        if (!data) {
            ret = -1;
            goto done;
        }
        data->url = strdup(url);
        data->flags = flags;
        data->uploadBuffer = webhandle->uploadBuffer;
        free(url);
        ret = pthread_create(&webhandle->connThread, NULL, writeThreadOperation, data);
        if (ret) {
            fprintf(stderr, "Failed to create the writing thread.\n");
        } else {
            webhandle->uploadBuffer->openFlag = 1;
        }
    }
    
done:
    if (ret == 0) {
        return hdfsFileHandle;
    } else {
        freeWebFileHandle(webhandle);
        free(hdfsFileHandle);
        return NULL;
    }
}

tSize hdfsWrite(hdfsFS fs, hdfsFile file, const void* buffer, tSize length)
{
    if (length == 0) {
        return 0;
    }
    if (fs == NULL || file == NULL || file->type != OUTPUT || length < 0) {
        return -1;
    }
    
    struct webhdfsFileHandle *wfile = (struct webhdfsFileHandle *) file->file;
    if (wfile->uploadBuffer && wfile->uploadBuffer->openFlag) {
        resetWebhdfsBuffer(wfile->uploadBuffer, buffer, length);
        return length;
    } else {
        fprintf(stderr, "Error: have not opened the file %s for writing yet.\n", wfile->absPath);
        return -1;
    }
}

int hdfsCloseFile(hdfsFS fs, hdfsFile file)
{
    int ret = 0;
    fprintf(stderr, "to close file...\n");
    if (file->type == OUTPUT) {
        void *respv;
        threadData *tdata;
        struct webhdfsFileHandle *wfile = (struct webhdfsFileHandle *) file->file;
        pthread_mutex_lock(&(wfile->uploadBuffer->writeMutex));
        wfile->uploadBuffer->closeFlag = 1;
        pthread_cond_signal(&wfile->uploadBuffer->newwrite_or_close);
        pthread_mutex_unlock(&(wfile->uploadBuffer->writeMutex));
        
        //waiting for the writing thread to terminate
        ret = pthread_join(wfile->connThread, &respv);
        if (ret) {
            fprintf(stderr, "Error (code %d) when pthread_join.\n", ret);
        }
        //parse the response
        tdata = (threadData *) respv;
        if (!tdata) {
            fprintf(stderr, "Response from the writing thread is NULL.\n");
            ret = -1;
        }
        if (file->flags & O_APPEND) {
            parseDnAPPEND(tdata->resp->header->content, tdata->resp->body->content);
        } else {
            parseDnWRITE(tdata->resp->header->content, tdata->resp->body->content);
        }
        //free the threaddata
        freeThreadData(tdata);
    }
    
    fprintf(stderr, "To clean the webfilehandle...\n");
    if (file) {
        freeWebFileHandle(file->file);
        free(file);
        file = NULL;
        fprintf(stderr, "Cleaned the webfilehandle...\n");
    }
    return ret;
}

int hdfsFileIsOpenForRead(hdfsFile file)
{
    return (file->type == INPUT);
}

int hdfsFileIsOpenForWrite(hdfsFile file)
{
    return (file->type == OUTPUT);
}

tSize hdfsRead(hdfsFS fs, hdfsFile file, void* buffer, tSize length)
{
    if (length == 0) {
        return 0;
    }
    if (fs == NULL || file == NULL || file->type != INPUT || buffer == NULL || length < 0) {
        errno = EINVAL;
        return -1;
    }
    struct hdfsBuilder *bld = (struct hdfsBuilder *) fs;
    struct webhdfsFileHandle *webFile = (struct webhdfsFileHandle *) file->file;
    char *url = NULL;
    Response resp = NULL;
    int openResult = -1;
    
    resp = (Response) calloc(1, sizeof(*resp));
    if (!resp) {
        return -1;
    }
    resp->header = initResponseBuffer();
    resp->body = initResponseBuffer();
    resp->body->content = buffer;
    resp->body->remaining = length;
    
    if (!((url = prepareOPEN(bld->nn, bld->port, webFile->absPath, bld->userName, file->offset, length))
          && (resp = launchOPEN(url, resp))
          && ((openResult = parseOPEN(resp->header->content, resp->body->content)) > 0))) {
        free(url);
        freeResponseBuffer(resp->header);
        if (openResult == 0) {
            return 0;
        } else {
            return -1;
        }
    }
    
    size_t readSize = resp->body->offset;
    file->offset += readSize;
    
    freeResponseBuffer(resp->header);
    free(resp->body);
    free(resp);
    free(url);
    return readSize;
}

int hdfsAvailable(hdfsFS fs, hdfsFile file)
{
    if (!file || !fs) {
        return -1;
    }
    struct webhdfsFileHandle *wf = (struct webhdfsFileHandle *) file->file;
    if (!wf) {
        return -1;
    }
    hdfsFileInfo *fileInfo = hdfsGetPathInfo(fs, wf->absPath);
    if (fileInfo) {
        int available = (int)(fileInfo->mSize - file->offset);
        hdfsFreeFileInfo(fileInfo, 1);
        return available;
    } else {
        return -1;
    }
}

int hdfsSeek(hdfsFS fs, hdfsFile file, tOffset desiredPos)
{
    if (!fs || !file || desiredPos < 0) {
        return -1;
    }
    struct webhdfsFileHandle *wf = (struct webhdfsFileHandle *) file->file;
    if (!wf) {
        return -1;
    }
    hdfsFileInfo *fileInfo = hdfsGetPathInfo(fs, wf->absPath);
    int ret = 0;
    if (fileInfo) {
        if (fileInfo->mSize < desiredPos) {
            errno = ENOTSUP;
            fprintf(stderr,
                    "hdfsSeek for %s failed since the desired position %lld is beyond the size of the file %lld\n",
                    wf->absPath, desiredPos, fileInfo->mSize);
            ret = -1;
        } else {
            file->offset = desiredPos;
        }
        hdfsFreeFileInfo(fileInfo, 1);
        return ret;
    } else {
        return -1;
    }
}

tSize hdfsPread(hdfsFS fs, hdfsFile file, tOffset position, void* buffer, tSize length)
{
    if (!fs || !file || file->type != INPUT || position < 0 || !buffer || length < 0) {
        return -1;
    }
    file->offset = position;
    return hdfsRead(fs, file, buffer, length);
}

tOffset hdfsTell(hdfsFS fs, hdfsFile file)
{
    if (!file) {
        return -1;
    }
    return file->offset;
}

char* hdfsGetWorkingDirectory(hdfsFS fs, char *buffer, size_t bufferSize)
{
    if (fs == NULL || buffer == NULL ||  bufferSize <= 0) {
        return NULL;
    }
    
    struct hdfsBuilder * bld = (struct hdfsBuilder *) fs;
    if (bld->workingDir) {
        strncpy(buffer, bld->workingDir, bufferSize);
    }
    return buffer;
}

int hdfsSetWorkingDirectory(hdfsFS fs, const char* path)
{
    if (fs == NULL || path == NULL) {
        return -1;
    }
    
    struct hdfsBuilder * bld = (struct hdfsBuilder *) fs;
    free(bld->workingDir);
    bld->workingDir = (char *)malloc(strlen(path) + 1);
    if (!(bld->workingDir)) {
        return -1;
    }
    strcpy(bld->workingDir, path);
    return 0;
}

void hdfsFreeHosts(char ***blockHosts)
{
    int i, j;
    for (i=0; blockHosts[i]; i++) {
        for (j=0; blockHosts[i][j]; j++) {
            free(blockHosts[i][j]);
        }
        free(blockHosts[i]);
    }
    free(blockHosts);
}

/* not useful for libwebhdfs */
int hdfsFileUsesDirectRead(hdfsFile file)
{
    /* return !!(file->flags & HDFS_FILE_SUPPORTS_DIRECT_READ); */
    fprintf(stderr, "hdfsFileUsesDirectRead is no longer useful for libwebhdfs.\n");
    return -1;
}

/* not useful for libwebhdfs */
void hdfsFileDisableDirectRead(hdfsFile file)
{
    /* file->flags &= ~HDFS_FILE_SUPPORTS_DIRECT_READ; */
    fprintf(stderr, "hdfsFileDisableDirectRead is no longer useful for libwebhdfs.\n");
}

/* not useful for libwebhdfs */
int hdfsHFlush(hdfsFS fs, hdfsFile file)
{
    return 0;
}

/* not useful for libwebhdfs */
int hdfsFlush(hdfsFS fs, hdfsFile file)
{
    return 0;
}

char*** hdfsGetHosts(hdfsFS fs, const char* path,
                     tOffset start, tOffset length)
{
    fprintf(stderr, "hdfsGetHosts is not but will be supported by libwebhdfs yet.\n");
    return NULL;
}

tOffset hdfsGetCapacity(hdfsFS fs)
{
    fprintf(stderr, "hdfsGetCapacity is not but will be supported by libwebhdfs.\n");
    return -1;
}

tOffset hdfsGetUsed(hdfsFS fs)
{
    fprintf(stderr, "hdfsGetUsed is not but will be supported by libwebhdfs yet.\n");
    return -1;
}

