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
#include <string.h>
#include <stdlib.h>

#include "exception.h"
#include "hdfs.h"
#include "hdfs_http_client.h"
#include "hdfs_http_query.h"
#include "hdfs_json_parser.h"
#include "jni_helper.h"

#define HADOOP_HDFS_CONF       "org/apache/hadoop/hdfs/HdfsConfiguration"
#define HADOOP_NAMENODE        "org/apache/hadoop/hdfs/server/namenode/NameNode"
#define JAVA_INETSOCKETADDRESS "java/net/InetSocketAddress"

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
    enum hdfsStreamType type;   /* INPUT or OUTPUT */
    int flags;                  /* Flag indicate read/create/append etc. */
    tOffset offset;             /* Current offset position in the file */
};

/**
 * Create, initialize and return a webhdfsBuffer
 */
static int initWebHdfsBuffer(struct webhdfsBuffer **webhdfsBuffer)
{
    int ret = 0;
    struct webhdfsBuffer *buffer = calloc(1, sizeof(struct webhdfsBuffer));
    if (!buffer) {
        fprintf(stderr,
                "ERROR: fail to allocate memory for webhdfsBuffer.\n");
        return ENOMEM;
    }
    ret = pthread_mutex_init(&buffer->writeMutex, NULL);
    if (ret) {
        fprintf(stderr, "ERROR: fail in pthread_mutex_init for writeMutex "
                "in initWebHdfsBuffer, <%d>: %s.\n",
                ret, hdfs_strerror(ret));
        goto done;
    }
    ret = pthread_cond_init(&buffer->newwrite_or_close, NULL);
    if (ret) {
        fprintf(stderr,
                "ERROR: fail in pthread_cond_init for newwrite_or_close "
                "in initWebHdfsBuffer, <%d>: %s.\n",
                ret, hdfs_strerror(ret));
        goto done;
    }
    ret = pthread_cond_init(&buffer->transfer_finish, NULL);
    if (ret) {
        fprintf(stderr,
                "ERROR: fail in pthread_cond_init for transfer_finish "
                "in initWebHdfsBuffer, <%d>: %s.\n",
                ret, hdfs_strerror(ret));
        goto done;
    }
    
done:
    if (ret) {
        free(buffer);
        return ret;
    }
    *webhdfsBuffer = buffer;
    return 0;
}

/**
 * Reset the webhdfsBuffer. This is used in a block way 
 * when hdfsWrite is called with a new buffer to write.
 * The writing thread in libcurl will be waken up to continue writing, 
 * and the caller of this function is blocked waiting for writing to finish.
 *
 * @param wb The handle of the webhdfsBuffer
 * @param buffer The buffer provided by user to write
 * @param length The length of bytes to write
 * @return Updated webhdfsBuffer.
 */
static struct webhdfsBuffer *resetWebhdfsBuffer(struct webhdfsBuffer *wb,
                                         const char *buffer, size_t length)
{
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

/**
 * Free the webhdfsBuffer and destroy its pthread conditions/mutex
 * @param buffer The webhdfsBuffer to free
 */
static void freeWebhdfsBuffer(struct webhdfsBuffer *buffer)
{
    int ret = 0;
    if (buffer) {
        ret = pthread_cond_destroy(&buffer->newwrite_or_close);
        if (ret) {
            fprintf(stderr,
                    "WARN: fail in pthread_cond_destroy for newwrite_or_close "
                    "in freeWebhdfsBuffer, <%d>: %s.\n",
                    ret, hdfs_strerror(ret));
            errno = ret;
        }
        ret = pthread_cond_destroy(&buffer->transfer_finish);
        if (ret) {
            fprintf(stderr,
                    "WARN: fail in pthread_cond_destroy for transfer_finish "
                    "in freeWebhdfsBuffer, <%d>: %s.\n",
                    ret, hdfs_strerror(ret));
            errno = ret;
        }
        ret = pthread_mutex_destroy(&buffer->writeMutex);
        if (ret) {
            fprintf(stderr,
                    "WARN: fail in pthread_mutex_destroy for writeMutex "
                    "in freeWebhdfsBuffer, <%d>: %s.\n",
                    ret, hdfs_strerror(ret));
            errno = ret;
        }
        free(buffer);
        buffer = NULL;
    }
}

/**
 * To free the webhdfsFileHandle, which includes a webhdfsBuffer and strings
 * @param handle The webhdfsFileHandle to free
 */
static void freeWebFileHandle(struct webhdfsFileHandle * handle)
{
    if (!handle)
        return;
    freeWebhdfsBuffer(handle->uploadBuffer);
    free(handle->datanode);
    free(handle->absPath);
    free(handle);
}

static const char *maybeNull(const char *str)
{
    return str ? str : "(NULL)";
}

/** To print a hdfsBuilder as string */
static const char *hdfsBuilderToStr(const struct hdfsBuilder *bld,
                                    char *buf, size_t bufLen)
{
    int strlength = snprintf(buf, bufLen, "nn=%s, port=%d, "
             "kerbTicketCachePath=%s, userName=%s",
             maybeNull(bld->nn), bld->port,
             maybeNull(bld->kerbTicketCachePath), maybeNull(bld->userName));
    if (strlength < 0 || strlength >= bufLen) {
        fprintf(stderr, "failed to print a hdfsBuilder as string.\n");
        return NULL;
    }
    return buf;
}

/**
 * Free a hdfs_internal handle
 * @param fs The hdfs_internal handle to free
 */
static void freeWebHdfsInternal(struct hdfs_internal *fs)
{
    if (fs) {
        free(fs->nn);
        free(fs->userName);
        free(fs->workingDir);
    }
}

struct hdfsBuilder *hdfsNewBuilder(void)
{
    struct hdfsBuilder *bld = calloc(1, sizeof(struct hdfsBuilder));
    if (!bld) {
        errno = ENOMEM;
        return NULL;
    }
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
    return hdfsConnect(nn, port);
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

/**
 * To retrieve the default configuration value for NameNode's hostName and port
 * TODO: This function currently is using JNI, 
 *       we need to do this without using JNI (HDFS-3917)
 *
 * @param bld The hdfsBuilder handle
 * @param port Used to get the default value for NameNode's port
 * @param nn Used to get the default value for NameNode's hostName
 * @return 0 for success and non-zero value for failure
 */
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
    
    env = getJNIEnv();
    if (!env) {
        return EINTERNAL;
    }
    
    jthr = constructNewObjectOfClass(env, &jHDFSConf, HADOOP_HDFS_CONF, "()V");
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                                    "hdfsBuilderConnect(%s)",
                                    hdfsBuilderToStr(bld, buf, sizeof(buf)));
        goto done;
    }
    
    jthr = invokeMethod(env, &jVal, STATIC, NULL,
        HADOOP_NAMENODE, "getHttpAddress",
        "(Lorg/apache/hadoop/conf/Configuration;)Ljava/net/InetSocketAddress;",
        jHDFSConf);
    if (jthr) {
        ret = printExceptionAndFree(env, jthr, PRINT_EXC_ALL,
                                    "hdfsBuilderConnect(%s)",
                                    hdfsBuilderToStr(bld, buf, sizeof(buf)));
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
                        JAVA_INETSOCKETADDRESS,
                        "getHostName", "()Ljava/lang/String;");
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
    int ret = 0;

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
    // If the namenode is "default" and/or the port of namenode is 0,
    // get the default namenode/port
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
    // For debug
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

/**
 * Based on the working directory stored in hdfsFS, 
 * generate the absolute path for the given path
 *
 * @param fs The hdfsFS handle which stores the current working directory
 * @param path The given path which may not be an absolute path
 * @param absPath To hold generated absolute path for the given path
 * @return 0 on success, non-zero value indicating error
 */
static int getAbsolutePath(hdfsFS fs, const char *path, char **absPath)
{
    char *tempPath = NULL;
    size_t absPathLen;
    int strlength;
    
    if (path[0] == '/') {
        // Path is already absolute.
        tempPath = strdup(path);
        if (!tempPath) {
            return ENOMEM;
        }
        *absPath = tempPath;
        return 0;
    }
    // Prepend the workingDir to the path.
    absPathLen = strlen(fs->workingDir) + strlen(path) + 1;
    tempPath = malloc(absPathLen);
    if (!tempPath) {
        return ENOMEM;
    }
    strlength = snprintf(tempPath, absPathLen, "%s%s", fs->workingDir, path);
    if (strlength < 0 || strlength >= absPathLen) {
        free(tempPath);
        return EIO;
    }
    *absPath = tempPath;
    return 0;
}

int hdfsCreateDirectory(hdfsFS fs, const char* path)
{
    char *url = NULL, *absPath = NULL;
    struct Response *resp = NULL;
    int ret = 0;

    if (fs == NULL || path == NULL) {
        ret = EINVAL;
        goto done;
    }
    ret = getAbsolutePath(fs, path, &absPath);
    if (ret) {
        goto done;
    }
    ret = createUrlForMKDIR(fs->nn, fs->port, absPath, fs->userName, &url);
    if (ret) {
        goto done;
    }
    ret = launchMKDIR(url, &resp);
    if (ret) {
        goto done;
    }
    ret = parseMKDIR(resp->body->content);
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
    struct Response *resp = NULL;
    int ret = 0;

    if (fs == NULL || path == NULL) {
        ret = EINVAL;
        goto done;
    }
    ret = getAbsolutePath(fs, path, &absPath);
    if (ret) {
        goto done;
    }
    ret = createUrlForCHMOD(fs->nn, fs->port, absPath, (int) mode,
                            fs->userName, &url);
    if (ret) {
        goto done;
    }
    ret = launchCHMOD(url, &resp);
    if (ret) {
        goto done;
    }
    ret = parseCHMOD(resp->header->content, resp->body->content);
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
    struct Response *resp = NULL;

    if (fs == NULL || path == NULL) {
        ret = EINVAL;
        goto done;
    }
    
    ret = getAbsolutePath(fs, path, &absPath);
    if (ret) {
        goto done;
    }
    ret = createUrlForCHOWN(fs->nn, fs->port, absPath,
                            owner, group, fs->userName, &url);
    if (ret) {
        goto done;
    }
    ret = launchCHOWN(url, &resp);
    if (ret) {
        goto done;
    }
    ret = parseCHOWN(resp->header->content, resp->body->content);
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
    struct Response *resp = NULL;

    if (fs == NULL || oldPath == NULL || newPath == NULL) {
        ret = EINVAL;
        goto done;
    }
    ret = getAbsolutePath(fs, oldPath, &oldAbsPath);
    if (ret) {
        goto done;
    }
    ret = getAbsolutePath(fs, newPath, &newAbsPath);
    if (ret) {
        goto done;
    }
    ret = createUrlForRENAME(fs->nn, fs->port, oldAbsPath,
                             newAbsPath, fs->userName, &url);
    if (ret) {
        goto done;
    }
    ret = launchRENAME(url, &resp);
    if (ret) {
        goto done;
    }
    ret = parseRENAME(resp->body->content);
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

/**
 * Get the file status for a given path. 
 * 
 * @param fs            hdfsFS handle containing 
 *                      NameNode hostName/port information
 * @param path          Path for file
 * @param printError    Whether or not to print out error information 
 *                      (mainly remote FileNotFoundException)
 * @return              File information for the given path
 */
static hdfsFileInfo *hdfsGetPathInfoImpl(hdfsFS fs, const char* path,
                                         int printError)
{
    char *absPath = NULL;
    char *url=NULL;
    struct Response *resp = NULL;
    int ret = 0;
    hdfsFileInfo *fileInfo = NULL;

    if (fs == NULL || path == NULL) {
        ret = EINVAL;
        goto done;
    }
    ret = getAbsolutePath(fs, path, &absPath);
    if (ret) {
        goto done;
    }
    fileInfo = (hdfsFileInfo *) calloc(1, sizeof(hdfsFileInfo));
    if (!fileInfo) {
        ret = ENOMEM;
        goto done;
    }
    fileInfo->mKind = kObjectKindFile;

    ret = createUrlForGetFileStatus(fs->nn, fs->port, absPath,
                                    fs->userName, &url);
    if (ret) {
        goto done;
    }
    ret = launchGFS(url, &resp);
    if (ret) {
        goto done;
    }
    ret = parseGFS(resp->body->content, fileInfo, printError);
    
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

hdfsFileInfo *hdfsGetPathInfo(hdfsFS fs, const char* path)
{
    return hdfsGetPathInfoImpl(fs, path, 1);
}

hdfsFileInfo *hdfsListDirectory(hdfsFS fs, const char* path, int *numEntries)
{
    char *url = NULL, *absPath = NULL;
    struct Response *resp = NULL;
    int ret = 0;
    hdfsFileInfo *fileInfo = NULL;

    if (fs == NULL || path == NULL) {
        ret = EINVAL;
        goto done;
    }
    ret = getAbsolutePath(fs, path, &absPath);
    if (ret) {
        goto done;
    }
    fileInfo = calloc(1, sizeof(*fileInfo));
    if (!fileInfo) {
        ret = ENOMEM;
        goto done;
    }
    
    ret = createUrlForLS(fs->nn, fs->port, absPath, fs->userName, &url);
    if (ret) {
        goto done;
    }
    ret = launchLS(url, &resp);
    if (ret) {
        goto done;
    }
    ret = parseLS(resp->body->content, &fileInfo, numEntries);
    
done:
    freeResponse(resp);
    free(absPath);
    free(url);
    if (ret == 0) {
        return fileInfo;
    } else {
        errno = ret;
        return NULL;
    }
}

int hdfsSetReplication(hdfsFS fs, const char* path, int16_t replication)
{
    char *url = NULL, *absPath = NULL;
    struct Response *resp = NULL;
    int ret = 0;

    if (fs == NULL || path == NULL) {
        ret = EINVAL;
        goto done;
    }
    ret = getAbsolutePath(fs, path, &absPath);
    if (ret) {
        goto done;
    }

    ret = createUrlForSETREPLICATION(fs->nn, fs->port, absPath,
                                     replication, fs->userName, &url);
    if (ret) {
        goto done;
    }
    ret = launchSETREPLICATION(url, &resp);
    if (ret) {
        goto done;
    }
    ret = parseSETREPLICATION(resp->body->content);
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
    for (i = 0; i < numEntries; ++i) {
        free(hdfsFileInfo[i].mName);
        free(hdfsFileInfo[i].mOwner);
        free(hdfsFileInfo[i].mGroup);
    }
    free(hdfsFileInfo);
}

int hdfsDelete(hdfsFS fs, const char* path, int recursive)
{
    char *url = NULL, *absPath = NULL;
    struct Response *resp = NULL;
    int ret = 0;

    if (fs == NULL || path == NULL) {
        ret = EINVAL;
        goto done;
    }
    ret = getAbsolutePath(fs, path, &absPath);
    if (ret) {
        goto done;
    }
    
    ret = createUrlForDELETE(fs->nn, fs->port, absPath,
                             recursive, fs->userName, &url);
    if (ret) {
        goto done;
    }
    ret = launchDELETE(url, &resp);
    if (ret) {
        goto done;
    }
    ret = parseDELETE(resp->body->content);
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
    struct Response *resp = NULL;
    int ret = 0;

    if (fs == NULL || path == NULL) {
        ret = EINVAL;
        goto done;
    }
    ret = getAbsolutePath(fs, path, &absPath);
    if (ret) {
        goto done;
    }
   
    ret = createUrlForUTIMES(fs->nn, fs->port, absPath, mtime, atime,
                             fs->userName, &url);
    if (ret) {
        goto done;
    }
    ret = launchUTIMES(url, &resp);
    if (ret) {
        goto done;
    }
    ret = parseUTIMES(resp->header->content, resp->body->content);
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
    hdfsFileInfo *fileInfo = hdfsGetPathInfoImpl(fs, path, 0);
    if (!fileInfo) {
        // (errno will have been set by hdfsGetPathInfo)
        return -1;
    }
    hdfsFreeFileInfo(fileInfo, 1);
    return 0;
}

/**
 * The information hold by the thread which writes data to hdfs through http
 */
typedef struct {
    char *url;          /* the url of the target datanode for writing*/
    struct webhdfsBuffer *uploadBuffer; /* buffer storing data to write */
    int flags;          /* flag indicating writing mode: create or append */
    struct Response *resp;      /* response from the target datanode */
} threadData;

/**
 * Free the threadData struct instance, 
 * including the response and url contained in it
 * @param data The threadData instance to free
 */
static void freeThreadData(threadData *data)
{
    if (data) {
        if (data->url) {
            free(data->url);
        }
        if (data->resp) {
            freeResponse(data->resp);
        }
        // The uploadBuffer would be freed by freeWebFileHandle()
        free(data);
        data = NULL;
    }
}

/**
 * The action of the thread that writes data to 
 * the target datanode for hdfsWrite. 
 * The writing can be either create or append, which is specified by flag
 */
static void *writeThreadOperation(void *v)
{
    int ret = 0;
    threadData *data = v;
    if (data->flags & O_APPEND) {
        ret = launchDnAPPEND(data->url, data->uploadBuffer, &(data->resp));
    } else {
        ret = launchDnWRITE(data->url, data->uploadBuffer, &(data->resp));
    }
    if (ret) {
        fprintf(stderr, "Failed to write to datanode %s, <%d>: %s.\n",
                data->url, ret, hdfs_strerror(ret));
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
    struct Response *resp = NULL;
    int append, ret = 0;
    char *nnUrl = NULL, *dnUrl = NULL;
    threadData *data = NULL;

    ret = initWebHdfsBuffer(&webhandle->uploadBuffer);
    if (ret) {
        goto done;
    }
    append = file->flags & O_APPEND;
    if (!append) {
        // If we're not appending, send a create request to the NN
        ret = createUrlForNnWRITE(fs->nn, fs->port, webhandle->absPath,
                                  fs->userName, webhandle->replication,
                                  webhandle->blockSize, &nnUrl);
    } else {
        ret = createUrlForNnAPPEND(fs->nn, fs->port, webhandle->absPath,
                                   fs->userName, &nnUrl);
    }
    if (ret) {
        fprintf(stderr, "Failed to create the url connecting to namenode "
                "for file creation/appending, <%d>: %s.\n",
                ret, hdfs_strerror(ret));
        goto done;
    }
    if (!append) {
        ret = launchNnWRITE(nnUrl, &resp);
    } else {
        ret = launchNnAPPEND(nnUrl, &resp);
    }
    if (ret) {
        fprintf(stderr, "fail to get the response from namenode for "
                "file creation/appending, <%d>: %s.\n",
                ret, hdfs_strerror(ret));
        goto done;
    }
    if (!append) {
        ret = parseNnWRITE(resp->header->content, resp->body->content);
    } else {
        ret = parseNnAPPEND(resp->header->content, resp->body->content);
    }
    if (ret) {
        fprintf(stderr, "fail to parse the response from namenode for "
                "file creation/appending, <%d>: %s.\n",
                ret, hdfs_strerror(ret));
        goto done;
    }
    ret = parseDnLoc(resp->header->content, &dnUrl);
    if (ret) {
        fprintf(stderr, "fail to get the datanode url from namenode "
                "for file creation/appending, <%d>: %s.\n",
                ret, hdfs_strerror(ret));
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
        fprintf(stderr, "ERROR: failed to create the writing thread "
                "in hdfsOpenOutputFileImpl, <%d>: %s.\n",
                ret, hdfs_strerror(ret));
        goto done;
    }
    webhandle->uploadBuffer->openFlag = 1;

done:
    freeResponse(resp);
    free(nnUrl);
    free(dnUrl);
    if (ret) {
        errno = ret;
        if (data) {
            free(data->url);
            free(data);
        }
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
        fprintf(stderr,
                "WARN: hdfs does not truly support O_CREATE && O_EXCL\n");
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
    ret = getAbsolutePath(fs, path, &webhandle->absPath);
    if (ret) {
        goto done;
    }
    file->file = webhandle;
    // If open for write/append,
    // open and keep the connection with the target datanode for writing
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
        fprintf(stderr,
                "Error: have not opened the file %s for writing yet.\n",
                wfile->absPath);
        errno = EBADF;
        return -1;
    }
}

int hdfsCloseFile(hdfsFS fs, hdfsFile file)
{
    void *respv = NULL;
    threadData *tdata = NULL;
    int ret = 0;
    struct webhdfsFileHandle *wfile = NULL;

    if (file->type == OUTPUT) {
        wfile = file->file;
        pthread_mutex_lock(&(wfile->uploadBuffer->writeMutex));
        wfile->uploadBuffer->closeFlag = 1;
        pthread_cond_signal(&wfile->uploadBuffer->newwrite_or_close);
        pthread_mutex_unlock(&(wfile->uploadBuffer->writeMutex));
        
        // Waiting for the writing thread to terminate
        ret = pthread_join(wfile->connThread, &respv);
        if (ret) {
            fprintf(stderr, "Error when pthread_join in hdfsClose, <%d>: %s.\n",
                    ret, hdfs_strerror(ret));
        }
        // Parse the response
        tdata = respv;
        if (!tdata || !(tdata->resp)) {
            fprintf(stderr,
                    "ERROR: response from the writing thread is NULL.\n");
            ret = EIO;
        }
        if (file->flags & O_APPEND) {
            ret = parseDnAPPEND(tdata->resp->header->content,
                                tdata->resp->body->content);
        } else {
            ret = parseDnWRITE(tdata->resp->header->content,
                               tdata->resp->body->content);
        }
        // Free the threaddata
        freeThreadData(tdata);
    }
    freeFileInternal(file);
    if (ret) {
        errno = ret;
        return -1;
    }
    return 0;
}

int hdfsFileIsOpenForRead(hdfsFile file)
{
    return (file->type == INPUT);
}

int hdfsFileGetReadStatistics(hdfsFile file,
                              struct hdfsReadStatistics **stats)
{
    errno = ENOTSUP;
    return -1;
}

int64_t hdfsReadStatisticsGetRemoteBytesRead(
                            const struct hdfsReadStatistics *stats)
{
  return stats->totalBytesRead - stats->totalLocalBytesRead;
}

void hdfsFileFreeReadStatistics(struct hdfsReadStatistics *stats)
{
    free(stats);
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
    struct Response *resp = NULL;

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
    ret = initResponseBuffer(&(resp->header));
    if (ret) {
        goto done;
    }
    ret = initResponseBuffer(&(resp->body));
    if (ret) {
        goto done;
    }
    memset(buffer, 0, length);
    resp->body->content = buffer;
    resp->body->remaining = length;
    
    ret = createUrlForOPEN(fs->nn, fs->port, file->file->absPath,
                           fs->userName, off, length, &url);
    if (ret) {
        goto done;
    }
    ret = launchOPEN(url, resp);
    if (ret) {
        goto done;
    }
    ret = parseOPEN(resp->header->content, resp->body->content);
    if (ret == -1) {
        // Special case: if parseOPEN returns -1, we asked for a byte range
        // with outside what the file contains.  In this case, hdfsRead and
        // hdfsPread return 0, meaning end-of-file.
        *numRead = 0;
    } else if (ret == 0) {
        *numRead = (tSize) resp->body->offset;
    }
done:
    if (resp) {
        freeResponseBuffer(resp->header);
        free(resp->body);
    }
    free(resp);
    free(url);
    return ret;
}

tSize hdfsRead(hdfsFS fs, hdfsFile file, void* buffer, tSize length)
{
    int ret = 0;
    tSize numRead = 0;

    ret = hdfsReadImpl(fs, file, buffer, (tSize) file->offset,
                       length, &numRead);
    if (ret > 0) {  // ret == -1 means end of file
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

tSize hdfsPread(hdfsFS fs, hdfsFile file, tOffset position,
                void* buffer, tSize length)
{
    int ret;
    tSize numRead = 0;

    if (position < 0) {
        errno = EINVAL;
        return -1;
    }
    ret = hdfsReadImpl(fs, file, buffer, (tSize) position, length, &numRead);
    if (ret > 0) {
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
    int strlength;
    if (fs == NULL || buffer == NULL ||  bufferSize <= 0) {
        errno = EINVAL;
        return NULL;
    }
    strlength = snprintf(buffer, bufferSize, "%s", fs->workingDir);
    if (strlength >= bufferSize) {
        errno = ENAMETOOLONG;
        return NULL;
    } else if (strlength < 0) {
        errno = EIO;
        return NULL;
    }
    return buffer;
}

/** Replace "//" with "/" in path */
static void normalizePath(char *path)
{
    int i = 0, j = 0, sawslash = 0;
    
    for (i = j = sawslash = 0; path[i] != '\0'; i++) {
        if (path[i] != '/') {
            sawslash = 0;
            path[j++] = path[i];
        } else if (path[i] == '/' && !sawslash) {
            sawslash = 1;
            path[j++] = '/';
        }
    }
    path[j] = '\0';
}

int hdfsSetWorkingDirectory(hdfsFS fs, const char* path)
{
    char *newWorkingDir = NULL;
    size_t strlenPath = 0, newWorkingDirLen = 0;
    int strlength;

    if (fs == NULL || path == NULL) {
        errno = EINVAL;
        return -1;
    }
    strlenPath = strlen(path);
    if (strlenPath < 1) {
        errno = EINVAL;
        return -1;
    }
    // the max string length of the new working dir is
    // (length of old working dir) + (length of given path) + strlen("/") + 1
    newWorkingDirLen = strlen(fs->workingDir) + strlenPath + 2;
    newWorkingDir = malloc(newWorkingDirLen);
    if (!newWorkingDir) {
        errno = ENOMEM;
        return -1;
    }
    strlength = snprintf(newWorkingDir, newWorkingDirLen, "%s%s%s",
                         (path[0] == '/') ? "" : fs->workingDir,
                         path, (path[strlenPath - 1] == '/') ? "" : "/");
    if (strlength < 0 || strlength >= newWorkingDirLen) {
        free(newWorkingDir);
        errno = EIO;
        return -1;
    }
    
    if (strstr(path, "//")) {
        // normalize the path by replacing "//" with "/"
        normalizePath(newWorkingDir);
    }
    
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
    // TODO: block until our write buffer is flushed (HDFS-3952)
    return 0;
}

int hdfsFlush(hdfsFS fs, hdfsFile file)
{
    if (file->type != OUTPUT) {
        errno = EINVAL; 
        return -1;
    }
    // TODO: block until our write buffer is flushed (HDFS-3952)
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

