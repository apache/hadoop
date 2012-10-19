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

#include "exception.h"
#include "hdfs.h"
#include "hdfs_http_client.h"
#include "hdfs_http_query.h"
#include "hdfs_json_parser.h"
#include "jni_helper.h"

#include <inttypes.h>
#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define HADOOP_HDFS_CONF        "org/apache/hadoop/hdfs/HdfsConfiguration"
#define HADOOP_NAMENODE         "org/apache/hadoop/hdfs/server/namenode/NameNode"
#define JAVA_INETSOCKETADDRESS  "java/net/InetSocketAddress"

struct hdfsBuilder {
    int forceNewInstance;
    const char *nn;
    tPort port;
    const char *kerbTicketCachePath;
    const char *userName;
};

/**
 * The information required for accessing webhdfs,
 * including the network address of the namenode and the user name
 *
 * Unlike the string in hdfsBuilder, the strings in this structure are
 * dynamically allocated.  This structure will not be freed until we disconnect
 * from HDFS.
 */
struct hdfs_internal {
    char *nn;
    tPort port;
    char *userName;

    /**
     * Working directory -- stored with a trailing slash.
     */
    char *workingDir;
};

/**
 * The 'file-handle' to a file in hdfs.
 */
struct hdfsFile_internal {
    struct webhdfsFileHandle* file;
    enum hdfsStreamType type;
    int flags;
    tOffset offset;
};

static webhdfsBuffer *initWebHdfsBuffer(void)
{
    webhdfsBuffer *buffer = calloc(1, sizeof(*buffer));
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
    if (!handle)
        return;
    freeWebhdfsBuffer(handle->uploadBuffer);
    free(handle->datanode);
    free(handle->absPath);
    free(handle);
}

struct hdfsBuilder *hdfsNewBuilder(void)
{
    struct hdfsBuilder *bld = calloc(1, sizeof(struct hdfsBuilder));
    if (!bld)
        return NULL;
    return bld;
}

void hdfsFreeBuilder(struct hdfsBuilder *bld)
{
    free(bld);
}

void hdfsBuilderSetForceNewInstance(struct hdfsBuilder *bld)
{
    // We don't cache instances in libwebhdfs, so this is not applicable.
}

void hdfsBuilderSetNameNode(struct hdfsBuilder *bld, const char *nn)
{
    if (bld) {
        bld->nn = nn;
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
    return hdfsBuilderConnect(bld);
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

static const char *maybeNull(const char *str)
{
    return str ? str : "(NULL)";
}

static const char *hdfsBuilderToStr(const struct hdfsBuilder *bld,
                                    char *buf, size_t bufLen)
{
    snprintf(buf, bufLen, "nn=%s, port=%d, "
             "kerbTicketCachePath=%s, userName=%s",
             maybeNull(bld->nn), bld->port,
             maybeNull(bld->kerbTicketCachePath), maybeNull(bld->userName));
    return buf;
}

static void freeWebHdfsInternal(struct hdfs_internal *fs)
{
    if (fs) {
        free(fs->nn);
        free(fs->userName);
        free(fs->workingDir);
    }
}

static int retrieveDefaults(const struct hdfsBuilder *bld, tPort *port,
                            char **nn)
{
    JNIEnv *env = 0;
    jobject jHDFSConf = NULL, jAddress = NULL;
    jstring jHostName = NULL;
    jvalue jVal;
    jthrowable jthr = NULL;
    int ret = 0;
    char buf[512];
    
    // TODO: can we do this without using JNI?  See HDFS-3917
    env = getJNIEnv();
    if (!env) {
        return EINTERNAL;
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
        goto done;
    }
    jAddress = jVal.l;
    
    jthr = invokeMethod(env, &jVal, INSTANCE, jAddress,
                        JAVA_INETSOCKETADDRESS, "getPort", "()I");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                                    "hdfsBuilderConnect(%s)",
                                    hdfsBuilderToStr(bld, buf, sizeof(buf)));
        goto done;
    }
    *port = jVal.i;
    
    jthr = invokeMethod(env, &jVal, INSTANCE, jAddress,
                        JAVA_INETSOCKETADDRESS, "getHostName", "()Ljava/lang/String;");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                                    "hdfsBuilderConnect(%s)",
                                    hdfsBuilderToStr(bld, buf, sizeof(buf)));
        goto done;
    }
    jHostName = jVal.l;
    jthr = newCStr(env, jHostName, nn);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                                    "hdfsBuilderConnect(%s)",
                                    hdfsBuilderToStr(bld, buf, sizeof(buf)));
        goto done;
    }

done:
    destroyLocalReference(env, jHDFSConf);
    destroyLocalReference(env, jAddress);
    destroyLocalReference(env, jHostName);
    return ret;
}

hdfsFS hdfsBuilderConnect(struct hdfsBuilder *bld)
{
    struct hdfs_internal *fs = NULL;
    int ret;

    if (!bld) {
        ret = EINVAL;
        goto done;
    }
    if (bld->nn == NULL) {
        // In the JNI version of libhdfs this returns a LocalFileSystem.
        ret = ENOTSUP;
        goto done;
    }
    
    fs = calloc(1, sizeof(*fs));
    if (!fs) {
        ret = ENOMEM;
        goto done;
    }
    /* If the namenode is "default" and/or the port of namenode is 0, get the
     * default namenode/port */
    if (bld->port == 0 || !strcasecmp("default", bld->nn)) {
        ret = retrieveDefaults(bld, &fs->port, &fs->nn);
        if (ret)
            goto done;
    } else {
        fs->port = bld->port;
        fs->nn = strdup(bld->nn);
        if (!fs->nn) {
            ret = ENOMEM;
            goto done;
        }
    }
    if (bld->userName) {
        // userName may be NULL
        fs->userName = strdup(bld->userName);
        if (!fs->userName) {
            ret = ENOMEM;
            goto done;
        }
    }
    // The working directory starts out as root.
    fs->workingDir = strdup("/");
    if (!fs->workingDir) {
        ret = ENOMEM;
        goto done;
    }
    //for debug
    fprintf(stderr, "namenode: %s:%d\n", bld->nn, bld->port);

done:
    free(bld);
    if (ret) {
        freeWebHdfsInternal(fs);
        errno = ret;
        return NULL;
    }
    return fs;
}

int hdfsDisconnect(hdfsFS fs)
{
    if (fs == NULL) {
        errno = EINVAL;
        return -1;
    }
    freeWebHdfsInternal(fs);
    return 0;
}

static char *getAbsolutePath(hdfsFS fs, const char *path)
{
    char *absPath = NULL;
    size_t absPathLen;
    
    if (path[0] == '/') {
        // path is already absolute.
        return strdup(path);
    }
    // prepend the workingDir to the path.
    absPathLen = strlen(fs->workingDir) + strlen(path);
    absPath = malloc(absPathLen + 1);
    if (!absPath) {
        return NULL;
    }
    snprintf(absPath, absPathLen + 1, "%s%s", fs->workingDir, path);
    return absPath;
}

int hdfsCreateDirectory(hdfsFS fs, const char* path)
{
    char *url = NULL, *absPath = NULL;
    Response resp = NULL;
    int ret = 0;

    if (fs == NULL || path == NULL) {
        ret = EINVAL;
        goto done;
    }
    absPath = getAbsolutePath(fs, path);
    if (!absPath) {
        ret = ENOMEM;
        goto done;
    }
    if(!((url = prepareMKDIR(fs->nn, fs->port, absPath, fs->userName))
         && (resp = launchMKDIR(url))
         && (parseMKDIR(resp->body->content)))) {
        ret = EIO;
        goto done;
    }
    
done:
    freeResponse(resp);
    free(url);
    free(absPath);
    if (ret) {
        errno = ret;
        return -1;
    }
    return 0;
}

int hdfsChmod(hdfsFS fs, const char* path, short mode)
{
    char *absPath = NULL, *url = NULL;
    Response resp = NULL;
    int ret = 0;

    if (fs == NULL || path == NULL) {
        ret = EINVAL;
        goto done;
    }
    absPath = getAbsolutePath(fs, path);
    if (!absPath) {
        ret = ENOMEM;
        goto done;
    }
    if(!((url = prepareCHMOD(fs->nn, fs->port, absPath, (int)mode, fs->userName))
         && (resp = launchCHMOD(url))
         && (parseCHMOD(resp->header->content, resp->body->content)))) {
        ret = EIO;
        goto done;
    }
done:
    freeResponse(resp);
    free(absPath);
    free(url);
    if (ret) {
        errno = ret;
        return -1;
    }
    return 0;
}

int hdfsChown(hdfsFS fs, const char* path, const char *owner, const char *group)
{
    int ret = 0;
    char *absPath = NULL, *url = NULL;
    Response resp = NULL;

    if (fs == NULL || path == NULL) {
        ret = EINVAL;
        goto done;
    }
    
    absPath = getAbsolutePath(fs, path);
    if (!absPath) {
        ret = ENOMEM;
        goto done;
    }
    
    if(!((url = prepareCHOWN(fs->nn, fs->port, absPath, owner, group, fs->userName))
         && (resp = launchCHOWN(url))
         && (parseCHOWN(resp->header->content, resp->body->content)))) {
        ret = EIO;
        goto done;
    }
    
done:
    freeResponse(resp);
    free(absPath);
    free(url);
    if (ret) {
        errno = ret;
        return -1;
    }
    return 0;
}

int hdfsRename(hdfsFS fs, const char* oldPath, const char* newPath)
{
    char *oldAbsPath = NULL, *newAbsPath = NULL, *url = NULL;
    int ret = 0;
    Response resp = NULL;

    if (fs == NULL || oldPath == NULL || newPath == NULL) {
        ret = EINVAL;
        goto done;
    }
    oldAbsPath = getAbsolutePath(fs, oldPath);
    if (!oldAbsPath) {
        ret = ENOMEM;
        goto done;
    }
    newAbsPath = getAbsolutePath(fs, newPath);
    if (!newAbsPath) {
        ret = ENOMEM;
        goto done;
    }
    if(!((url = prepareRENAME(fs->nn, fs->port, oldAbsPath, newAbsPath, fs->userName))
         && (resp = launchRENAME(url))
         && (parseRENAME(resp->body->content)))) {
        ret = -1;
    }
done:
    freeResponse(resp);
    free(oldAbsPath);
    free(newAbsPath);
    free(url);
    if (ret) {
        errno = ret;
        return -1;
    }
    return 0;
}

hdfsFileInfo *hdfsGetPathInfo(hdfsFS fs, const char* path)
{
    char *absPath = NULL;
    char *url=NULL;
    Response resp = NULL;
    int numEntries = 0;
    int ret = 0;
    hdfsFileInfo *fileInfo = NULL;

    if (fs == NULL || path == NULL) {
        ret = EINVAL;
        goto done;
    }
    absPath = getAbsolutePath(fs, path);
    if (!absPath) {
        ret = ENOMEM;
        goto done;
    }
    fileInfo = (hdfsFileInfo *) calloc(1, sizeof(hdfsFileInfo));
    if (!fileInfo) {
        ret = ENOMEM;
        goto done;
    }
    fileInfo->mKind = kObjectKindFile;

    if(!((url = prepareGFS(fs->nn, fs->port, absPath, fs->userName))
         && (resp = launchGFS(url))
         && (fileInfo = parseGFS(resp->body->content, fileInfo, &numEntries))))  {
        ret = EIO;
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
        errno = ret;
        return NULL;
    }
}

hdfsFileInfo *hdfsListDirectory(hdfsFS fs, const char* path, int *numEntries)
{
    char *url = NULL, *absPath = NULL;
    Response resp = NULL;
    int ret = 0;
    hdfsFileInfo *fileInfo = NULL;

    if (fs == NULL || path == NULL) {
        ret = EINVAL;
        goto done;
    }
    absPath = getAbsolutePath(fs, path);
    if (!absPath) {
        ret = ENOMEM;
        goto done;
    }
    fileInfo = calloc(1, sizeof(*fileInfo));
    if (!fileInfo) {
        ret = ENOMEM;
        goto done;
    }
    if(!((url = prepareLS(fs->nn, fs->port, absPath, fs->userName))
         && (resp = launchLS(url))
         && (fileInfo = parseGFS(resp->body->content, fileInfo, numEntries))))  {
        ret = EIO;
        goto done;
    }
done:
    freeResponse(resp);
    free(absPath);
    free(url);

    if (ret == 0) {
        return fileInfo;
    } else {
        hdfsFreeFileInfo(fileInfo, 1);
        errno = ret;
        return NULL;
    }
}

int hdfsSetReplication(hdfsFS fs, const char* path, int16_t replication)
{
    char *url = NULL, *absPath = NULL;
    Response resp = NULL;
    int ret = 0;

    if (fs == NULL || path == NULL) {
        ret = EINVAL;
        goto done;
    }
    absPath = getAbsolutePath(fs, path);
    if (!absPath) {
        ret = ENOMEM;
        goto done;
    }
    if(!((url = prepareSETREPLICATION(fs->nn, fs->port, absPath, replication, fs->userName))
         && (resp = launchSETREPLICATION(url))
         && (parseSETREPLICATION(resp->body->content)))) {
        ret = EIO;
        goto done;
    }
done:
    freeResponse(resp);
    free(absPath);
    free(url);
    if (ret) {
        errno = ret;
        return -1;
    }
    return 0;
}

void hdfsFreeFileInfo(hdfsFileInfo *hdfsFileInfo, int numEntries)
{
    int i;

    for (i=0; i < numEntries; ++i) {
        free(hdfsFileInfo[i].mName);
        free(hdfsFileInfo[i].mOwner);
        free(hdfsFileInfo[i].mGroup);
    }
    free(hdfsFileInfo);
}

int hdfsDelete(hdfsFS fs, const char* path, int recursive)
{
    char *url = NULL, *absPath = NULL;
    Response resp = NULL;
    int ret = 0;

    if (fs == NULL || path == NULL) {
        ret = EINVAL;
        goto done;
    }
    absPath = getAbsolutePath(fs, path);
    if (!absPath) {
        ret = ENOMEM;
        goto done;
    }
    if(!((url = prepareDELETE(fs->nn, fs->port, absPath, recursive, fs->userName))
         && (resp = launchDELETE(url))
         && (parseDELETE(resp->body->content)))) {
        ret = EIO;
        goto done;
    }
    
done:
    freeResponse(resp);
    free(absPath);
    free(url);
    if (ret) {
        errno = ret;
        return -1;
    }
    return 0;
}

int hdfsUtime(hdfsFS fs, const char* path, tTime mtime, tTime atime)
{
    char *url = NULL, *absPath = NULL;
    Response resp = NULL;
    int ret = 0;

    if (fs == NULL || path == NULL) {
        ret = EINVAL;
        goto done;
    }
    absPath = getAbsolutePath(fs, path);
    if (!absPath) {
        ret = ENOMEM;
        goto done;
    }
    if(!((url = prepareUTIMES(fs->nn, fs->port, absPath, mtime, atime,
                              fs->userName))
         && (resp = launchUTIMES(url))
         && (parseUTIMES(resp->header->content, resp->body->content)))) {
        ret = EIO;
        goto done;
    }
    
done:
    freeResponse(resp);
    free(absPath);
    free(url);
    if (ret) {
        errno = ret;
        return -1;
    }
    return 0;
}

int hdfsExists(hdfsFS fs, const char *path)
{
    hdfsFileInfo *fileInfo = hdfsGetPathInfo(fs, path);
    if (!fileInfo) {
        // (errno will have been set by hdfsGetPathInfo)
        return -1;
    }
    hdfsFreeFileInfo(fileInfo, 1);
    return 0;
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

/**
 * Free the memory associated with a webHDFS file handle.
 *
 * No other resources will be freed.
 *
 * @param file            The webhdfs file handle
 */
static void freeFileInternal(hdfsFile file)
{
    if (!file)
        return;
    freeWebFileHandle(file->file);
    free(file);
}

/**
 * Helper function for opening a file for OUTPUT.
 *
 * As part of the open process for OUTPUT files, we have to connect to the
 * NameNode and get the URL of the corresponding DataNode.
 * We also create a background thread here for doing I/O.
 *
 * @param webhandle              The webhandle being opened
 * @return                       0 on success; error code otherwise
 */
static int hdfsOpenOutputFileImpl(hdfsFS fs, hdfsFile file)
{
    struct webhdfsFileHandle *webhandle = file->file;
    Response resp = NULL;
    int parseRet, append, ret = 0;
    char *prepareUrl = NULL, *dnUrl = NULL;
    threadData *data = NULL;

    webhandle->uploadBuffer = initWebHdfsBuffer();
    if (!webhandle->uploadBuffer) {
        ret = ENOMEM;
        goto done;
    }
    append = file->flags & O_APPEND;
    if (!append) {
        // If we're not appending, send a create request to the NN
        prepareUrl = prepareNnWRITE(fs->nn, fs->port, webhandle->absPath,
            fs->userName, webhandle->replication, webhandle->blockSize);
    } else {
        prepareUrl = prepareNnAPPEND(fs->nn, fs->port, webhandle->absPath,
                              fs->userName);
    }
    if (!prepareUrl) {
        fprintf(stderr, "fail to create the url connecting to namenode "
                "for file creation/appending\n");
        ret = EIO;
        goto done;
    }
    if (!append) {
        resp = launchNnWRITE(prepareUrl);
    } else {
        resp = launchNnAPPEND(prepareUrl);
    }
    if (!resp) {
        fprintf(stderr, "fail to get the response from namenode for "
                "file creation/appending\n");
        ret = EIO;
        goto done;
    }
    if (!append) {
        parseRet = parseNnWRITE(resp->header->content, resp->body->content);
    } else {
        parseRet = parseNnAPPEND(resp->header->content, resp->body->content);
    }
    if (!parseRet) {
        fprintf(stderr, "fail to parse the response from namenode for "
                "file creation/appending\n");
        ret = EIO;
        goto done;
    }
    dnUrl = parseDnLoc(resp->header->content);
    if (!dnUrl) {
        fprintf(stderr, "fail to get the datanode url from namenode "
                "for file creation/appending\n");
        ret = EIO;
        goto done;
    }
    //store the datanode url in the file handle
    webhandle->datanode = strdup(dnUrl);
    if (!webhandle->datanode) {
        ret = ENOMEM;
        goto done;
    }
    //create a new thread for performing the http transferring
    data = calloc(1, sizeof(*data));
    if (!data) {
        ret = ENOMEM;
        goto done;
    }
    data->url = strdup(dnUrl);
    if (!data->url) {
        ret = ENOMEM;
        goto done;
    }
    data->flags = file->flags;
    data->uploadBuffer = webhandle->uploadBuffer;
    ret = pthread_create(&webhandle->connThread, NULL,
                         writeThreadOperation, data);
    if (ret) {
        fprintf(stderr, "Failed to create the writing thread.\n");
        goto done;
    }
    webhandle->uploadBuffer->openFlag = 1;

done:
    freeResponse(resp);
    free(prepareUrl);
    free(dnUrl);
    if (ret) {
        free(data->url);
        free(data);
    }
    return ret;
}

hdfsFile hdfsOpenFile(hdfsFS fs, const char* path, int flags,
                      int bufferSize, short replication, tSize blockSize)
{
    int ret = 0;
    int accmode = flags & O_ACCMODE;
    struct webhdfsFileHandle *webhandle = NULL;
    hdfsFile file = NULL;

    if (fs == NULL || path == NULL) {
        ret = EINVAL;
        goto done;
    }
    if (accmode == O_RDWR) {
        // TODO: the original libhdfs has very hackish support for this; should
        // we do the same?  It would actually be a lot easier in libwebhdfs
        // since the protocol isn't connection-oriented. 
        fprintf(stderr, "ERROR: cannot open an hdfs file in O_RDWR mode\n");
        ret = ENOTSUP;
        goto done;
    }
    if ((flags & O_CREAT) && (flags & O_EXCL)) {
        fprintf(stderr, "WARN: hdfs does not truly support O_CREATE && O_EXCL\n");
    }
    file = calloc(1, sizeof(struct hdfsFile_internal));
    if (!file) {
        ret = ENOMEM;
        goto done;
    }
    file->flags = flags;
    file->type = accmode == O_RDONLY ? INPUT : OUTPUT;
    file->offset = 0;
    webhandle = calloc(1, sizeof(struct webhdfsFileHandle));
    if (!webhandle) {
        ret = ENOMEM;
        goto done;
    }
    webhandle->bufferSize = bufferSize;
    webhandle->replication = replication;
    webhandle->blockSize = blockSize;
    webhandle->absPath = getAbsolutePath(fs, path);
    if (!webhandle->absPath) {
        ret = ENOMEM;
        goto done;
    }
    file->file = webhandle;
    if (file->type == OUTPUT) {
        ret = hdfsOpenOutputFileImpl(fs, file);
        if (ret) {
            goto done;
        }
    }

done:
    if (ret) {
        if (file) {
            freeFileInternal(file); // Also frees webhandle
        } else {
            freeWebFileHandle(webhandle);
        }
        errno = ret;
        return NULL;
    }
    return file;
}

tSize hdfsWrite(hdfsFS fs, hdfsFile file, const void* buffer, tSize length)
{
    if (length == 0) {
        return 0;
    }
    if (fs == NULL || file == NULL || file->type != OUTPUT || length < 0) {
        errno = EBADF;
        return -1;
    }
    
    struct webhdfsFileHandle *wfile = file->file;
    if (wfile->uploadBuffer && wfile->uploadBuffer->openFlag) {
        resetWebhdfsBuffer(wfile->uploadBuffer, buffer, length);
        return length;
    } else {
        fprintf(stderr, "Error: have not opened the file %s for writing yet.\n", wfile->absPath);
        errno = EBADF;
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
        struct webhdfsFileHandle *wfile = file->file;
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
    freeFileInternal(file);
    fprintf(stderr, "Closed the webfilehandle...\n");
    if (ret) {
        errno = EIO;
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

static int hdfsReadImpl(hdfsFS fs, hdfsFile file, void* buffer, tSize off,
                        tSize length, tSize *numRead)
{
    int ret = 0;
    char *url = NULL;
    Response resp = NULL;
    int openResult = -1;

    if (fs == NULL || file == NULL || file->type != INPUT || buffer == NULL ||
            length < 0) {
        ret = EINVAL;
        goto done;
    }
    if (length == 0) {
        // Special case: the user supplied a buffer of zero length, so there is
        // nothing to do.
        *numRead = 0;
        goto done;
    }
    resp = calloc(1, sizeof(*resp)); // resp is actually a pointer type
    if (!resp) {
        ret = ENOMEM;
        goto done;
    }
    resp->header = initResponseBuffer();
    resp->body = initResponseBuffer();
    resp->body->content = buffer;
    resp->body->remaining = length;
    
    if (!((url = prepareOPEN(fs->nn, fs->port, file->file->absPath,
                             fs->userName, off, length))
          && (resp = launchOPEN(url, resp))
          && ((openResult = parseOPEN(resp->header->content, resp->body->content)) > 0))) {
        if (openResult == 0) {
            // Special case: if parseOPEN returns 0, we asked for a byte range
            // with outside what the file contains.  In this case, hdfsRead and
            // hdfsPread return 0, meaning end-of-file.
            *numRead = 0;
            goto done;
        }
        ret = EIO;
        goto done;
    }
    *numRead = resp->body->offset;

done:
    freeResponseBuffer(resp->header);
    free(resp->body);
    free(resp);
    free(url);
    return ret;
}

tSize hdfsRead(hdfsFS fs, hdfsFile file, void* buffer, tSize length)
{
    int ret;
    tSize numRead = 0;

    ret = hdfsReadImpl(fs, file, buffer, file->offset, length, &numRead);
    if (ret) {
        errno = ret;
        return -1;
    }
    file->offset += numRead; 
    return numRead;
}

int hdfsAvailable(hdfsFS fs, hdfsFile file)
{
    /* We actually always block when reading from webhdfs, currently.  So the
     * number of bytes that can be read without blocking is currently 0.
     */
    return 0;
}

int hdfsCopy(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst)
{
    errno = ENOTSUP;
    return -1;
}

int hdfsMove(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst)
{
    errno = ENOTSUP;
    return -1;
}

int hdfsSeek(hdfsFS fs, hdfsFile file, tOffset desiredPos)
{
    struct webhdfsFileHandle *wf;
    hdfsFileInfo *fileInfo = NULL;
    int ret = 0;

    if (!fs || !file || (file->type == OUTPUT) || (desiredPos < 0)) {
        ret = EINVAL;
        goto done;
    }
    wf = file->file;
    if (!wf) {
        ret = EINVAL;
        goto done;
    }
    fileInfo = hdfsGetPathInfo(fs, wf->absPath);
    if (!fileInfo) {
        ret = errno;
        goto done;
    }
    if (desiredPos > fileInfo->mSize) {
        fprintf(stderr,
                "hdfsSeek for %s failed since the desired position %" PRId64
                " is beyond the size of the file %" PRId64 "\n",
                wf->absPath, desiredPos, fileInfo->mSize);
        ret = ENOTSUP;
        goto done;
    }
    file->offset = desiredPos;

done:
    if (fileInfo) {
        hdfsFreeFileInfo(fileInfo, 1);
    }
    if (ret) {
        errno = ret;
        return -1;
    }
    return 0;
}

tSize hdfsPread(hdfsFS fs, hdfsFile file, tOffset position, void* buffer, tSize length)
{
    int ret;
    tSize numRead = 0;

    if (position < 0) {
        errno = EINVAL;
        return -1;
    }
    ret = hdfsReadImpl(fs, file, buffer, position, length, &numRead);
    if (ret) {
        errno = ret;
        return -1;
    }
    return numRead;
}

tOffset hdfsTell(hdfsFS fs, hdfsFile file)
{
    if (!file) {
        errno = EINVAL;
        return -1;
    }
    return file->offset;
}

char* hdfsGetWorkingDirectory(hdfsFS fs, char *buffer, size_t bufferSize)
{
    if (fs == NULL || buffer == NULL ||  bufferSize <= 0) {
        errno = EINVAL;
        return NULL;
    }
    if (snprintf(buffer, bufferSize, "%s", fs->workingDir) >= bufferSize) {
        errno = ENAMETOOLONG;
        return NULL;
    }
    return buffer;
}

int hdfsSetWorkingDirectory(hdfsFS fs, const char* path)
{
    char *newWorkingDir;
    size_t strlenPath, newWorkingDirLen;

    if (fs == NULL || path == NULL) {
        errno = EINVAL;
        return -1;
    }
    strlenPath = strlen(path);
    if (strlenPath < 1) {
        errno = EINVAL;
        return -1;
    }
    if (path[0] != '/') {
        // TODO: support non-absolute paths.  They should be interpreted
        // relative to the current path.
        errno = ENOTSUP;
        return -1;
    }
    if (strstr(path, "//")) {
        // TODO: support non-normalized paths (by normalizing them.)
        errno = ENOTSUP;
        return -1;
    }
    newWorkingDirLen = strlenPath + 2;
    newWorkingDir = malloc(newWorkingDirLen);
    if (!newWorkingDir) {
        errno = ENOMEM;
        return -1;
    }
    snprintf(newWorkingDir, newWorkingDirLen, "%s%s",
             path, (path[strlenPath - 1] == '/') ? "" : "/");
    free(fs->workingDir);
    fs->workingDir = newWorkingDir;
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

tOffset hdfsGetDefaultBlockSize(hdfsFS fs)
{
    errno = ENOTSUP;
    return -1;
}

int hdfsFileUsesDirectRead(hdfsFile file)
{
    return 0; // webhdfs never performs direct reads.
}

void hdfsFileDisableDirectRead(hdfsFile file)
{
    // webhdfs never performs direct reads
}

int hdfsHFlush(hdfsFS fs, hdfsFile file)
{
    if (file->type != OUTPUT) {
        errno = EINVAL; 
        return -1;
    }
    // TODO: block until our write buffer is flushed
    return 0;
}

int hdfsFlush(hdfsFS fs, hdfsFile file)
{
    if (file->type != OUTPUT) {
        errno = EINVAL; 
        return -1;
    }
    // TODO: block until our write buffer is flushed
    return 0;
}

char*** hdfsGetHosts(hdfsFS fs, const char* path,
                     tOffset start, tOffset length)
{
    errno = ENOTSUP;
    return NULL;
}

tOffset hdfsGetCapacity(hdfsFS fs)
{
    errno = ENOTSUP;
    return -1;
}

tOffset hdfsGetUsed(hdfsFS fs)
{
    errno = ENOTSUP;
    return -1;
}

