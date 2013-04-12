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
#include "hdfs_http_query.h"
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

#define PERM_STR_LEN 4  // "644" + one byte for NUL
#define SHORT_STR_LEN 6 // 65535 + NUL
#define LONG_STR_LEN 21 // 2^64-1 = 18446744073709551615 + NUL

/**
 * Create query based on NameNode hostname,
 * NameNode port, path, operation and other parameters
 *
 * @param host          NameNode hostName
 * @param nnPort        Port of NameNode
 * @param path          Absolute path for the corresponding file
 * @param op            Operations
 * @param paraNum       Number of remaining parameters
 * @param paraNames     Names of remaining parameters
 * @param paraValues    Values of remaining parameters
 * @param url           Holding the created URL
 * @return 0 on success and non-zero value to indicate error
 */
static int createQueryURL(const char *host, unsigned int nnPort,
                          const char *path, const char *op, int paraNum,
                          const char **paraNames, const char **paraValues,
                          char **queryUrl)
{
    size_t length = 0;
    int i = 0, offset = 0, ret = 0;
    char *url = NULL;
    const char *protocol = "http://";
    const char *prefix = "/webhdfs/v1";
    
    if (!paraNames || !paraValues) {
        return EINVAL;
    }
    length = strlen(protocol) + strlen(host) + strlen(":") +
                SHORT_STR_LEN + strlen(prefix) + strlen(path) +
                strlen ("?op=") + strlen(op);
    for (i = 0; i < paraNum; i++) {
        if (paraNames[i] && paraValues[i]) {
            length += 2 + strlen(paraNames[i]) + strlen(paraValues[i]);
        }
    }
    url = malloc(length);   // The '\0' has already been included
                            // when using SHORT_STR_LEN
    if (!url) {
        return ENOMEM;
    }
    
    offset = snprintf(url, length, "%s%s:%d%s%s?op=%s",
                      protocol, host, nnPort, prefix, path, op);
    if (offset >= length || offset < 0) {
        ret = EIO;
        goto done;
    }
    for (i = 0; i < paraNum; i++) {
        if (!paraNames[i] || !paraValues[i] || paraNames[i][0] == '\0' ||
            paraValues[i][0] == '\0') {
            continue;
        }
        offset += snprintf(url + offset, length - offset,
                           "&%s=%s", paraNames[i], paraValues[i]);
        if (offset >= length || offset < 0) {
            ret = EIO;
            goto done;
        }
    }
done:
    if (ret) {
        free(url);
        return ret;
    }
    *queryUrl = url;
    return 0;
}

int createUrlForMKDIR(const char *host, int nnPort,
                      const char *path, const char *user, char **url)
{
    const char *userPara = "user.name";
    return createQueryURL(host, nnPort, path, "MKDIRS", 1,
                          &userPara, &user, url);
}

int createUrlForGetFileStatus(const char *host, int nnPort, const char *path,
                              const char *user, char **url)
{
    const char *userPara = "user.name";
    return createQueryURL(host, nnPort, path, "GETFILESTATUS", 1,
                          &userPara, &user, url);
}

int createUrlForLS(const char *host, int nnPort, const char *path,
                   const char *user, char **url)
{
    const char *userPara = "user.name";
    return createQueryURL(host, nnPort, path, "LISTSTATUS",
                          1, &userPara, &user, url);
}

int createUrlForNnAPPEND(const char *host, int nnPort, const char *path,
                         const char *user, char **url)
{
    const char *userPara = "user.name";
    return createQueryURL(host, nnPort, path, "APPEND",
                          1, &userPara, &user, url);
}

int createUrlForMKDIRwithMode(const char *host, int nnPort, const char *path,
                              int mode, const char *user, char **url)
{
    int strlength;
    char permission[PERM_STR_LEN];
    const char *paraNames[2], *paraValues[2];
    
    paraNames[0] = "permission";
    paraNames[1] = "user.name";
    memset(permission, 0, PERM_STR_LEN);
    strlength = snprintf(permission, PERM_STR_LEN, "%o", mode);
    if (strlength < 0 || strlength >= PERM_STR_LEN) {
        return EIO;
    }
    paraValues[0] = permission;
    paraValues[1] = user;
    
    return createQueryURL(host, nnPort, path, "MKDIRS", 2,
                          paraNames, paraValues, url);
}

int createUrlForRENAME(const char *host, int nnPort, const char *srcpath,
                         const char *destpath, const char *user, char **url)
{
    const char *paraNames[2], *paraValues[2];
    paraNames[0] = "destination";
    paraNames[1] = "user.name";
    paraValues[0] = destpath;
    paraValues[1] = user;
    
    return createQueryURL(host, nnPort, srcpath,
                          "RENAME", 2, paraNames, paraValues, url);
}

int createUrlForCHMOD(const char *host, int nnPort, const char *path,
                      int mode, const char *user, char **url)
{
    int strlength;
    char permission[PERM_STR_LEN];
    const char *paraNames[2], *paraValues[2];
    
    paraNames[0] = "permission";
    paraNames[1] = "user.name";
    memset(permission, 0, PERM_STR_LEN);
    strlength = snprintf(permission, PERM_STR_LEN, "%o", mode);
    if (strlength < 0 || strlength >= PERM_STR_LEN) {
        return EIO;
    }
    paraValues[0] = permission;
    paraValues[1] = user;
    
    return createQueryURL(host, nnPort, path, "SETPERMISSION",
                          2, paraNames, paraValues, url);
}

int createUrlForDELETE(const char *host, int nnPort, const char *path,
                       int recursive, const char *user, char **url)
{
    const char *paraNames[2], *paraValues[2];
    paraNames[0] = "recursive";
    paraNames[1] = "user.name";
    if (recursive) {
        paraValues[0] = "true";
    } else {
        paraValues[0] = "false";
    }
    paraValues[1] = user;
    
    return createQueryURL(host, nnPort, path, "DELETE",
                          2, paraNames, paraValues, url);
}

int createUrlForCHOWN(const char *host, int nnPort, const char *path,
                      const char *owner, const char *group,
                      const char *user, char **url)
{
    const char *paraNames[3], *paraValues[3];
    paraNames[0] = "owner";
    paraNames[1] = "group";
    paraNames[2] = "user.name";
    paraValues[0] = owner;
    paraValues[1] = group;
    paraValues[2] = user;
    
    return createQueryURL(host, nnPort, path, "SETOWNER",
                          3, paraNames, paraValues, url);
}

int createUrlForOPEN(const char *host, int nnPort, const char *path,
                     const char *user, size_t offset, size_t length, char **url)
{
    int strlength;
    char offsetStr[LONG_STR_LEN], lengthStr[LONG_STR_LEN];
    const char *paraNames[3], *paraValues[3];
    
    paraNames[0] = "offset";
    paraNames[1] = "length";
    paraNames[2] = "user.name";
    memset(offsetStr, 0, LONG_STR_LEN);
    memset(lengthStr, 0, LONG_STR_LEN);
    strlength = snprintf(offsetStr, LONG_STR_LEN, "%lu", offset);
    if (strlength < 0 || strlength >= LONG_STR_LEN) {
        return EIO;
    }
    strlength = snprintf(lengthStr, LONG_STR_LEN, "%lu", length);
    if (strlength < 0 || strlength >= LONG_STR_LEN) {
        return EIO;
    }
    paraValues[0] = offsetStr;
    paraValues[1] = lengthStr;
    paraValues[2] = user;
    
    return createQueryURL(host, nnPort, path, "OPEN",
                          3, paraNames, paraValues, url);
}

int createUrlForUTIMES(const char *host, int nnPort, const char *path,
                       long unsigned mTime, long unsigned aTime,
                       const char *user, char **url)
{
    int strlength;
    char modTime[LONG_STR_LEN], acsTime[LONG_STR_LEN];
    const char *paraNames[3], *paraValues[3];
    
    memset(modTime, 0, LONG_STR_LEN);
    memset(acsTime, 0, LONG_STR_LEN);
    strlength = snprintf(modTime, LONG_STR_LEN, "%lu", mTime);
    if (strlength < 0 || strlength >= LONG_STR_LEN) {
        return EIO;
    }
    strlength = snprintf(acsTime, LONG_STR_LEN, "%lu", aTime);
    if (strlength < 0 || strlength >= LONG_STR_LEN) {
        return EIO;
    }
    paraNames[0] = "modificationtime";
    paraNames[1] = "accesstime";
    paraNames[2] = "user.name";
    paraValues[0] = modTime;
    paraValues[1] = acsTime;
    paraValues[2] = user;
    
    return createQueryURL(host, nnPort, path, "SETTIMES",
                          3, paraNames, paraValues, url);
}

int createUrlForNnWRITE(const char *host, int nnPort,
                        const char *path, const char *user,
                        int16_t replication, size_t blockSize, char **url)
{
    int strlength;
    char repStr[SHORT_STR_LEN], blockSizeStr[LONG_STR_LEN];
    const char *paraNames[4], *paraValues[4];
    
    memset(repStr, 0, SHORT_STR_LEN);
    memset(blockSizeStr, 0, LONG_STR_LEN);
    if (replication > 0) {
        strlength = snprintf(repStr, SHORT_STR_LEN, "%u", replication);
        if (strlength < 0 || strlength >= SHORT_STR_LEN) {
            return EIO;
        }
    }
    if (blockSize > 0) {
        strlength = snprintf(blockSizeStr, LONG_STR_LEN, "%lu", blockSize);
        if (strlength < 0 || strlength >= LONG_STR_LEN) {
            return EIO;
        }
    }
    paraNames[0] = "overwrite";
    paraNames[1] = "replication";
    paraNames[2] = "blocksize";
    paraNames[3] = "user.name";
    paraValues[0] = "true";
    paraValues[1] = repStr;
    paraValues[2] = blockSizeStr;
    paraValues[3] = user;
    
    return createQueryURL(host, nnPort, path, "CREATE",
                          4, paraNames, paraValues, url);
}

int createUrlForSETREPLICATION(const char *host, int nnPort,
                               const char *path, int16_t replication,
                               const char *user, char **url)
{
    char repStr[SHORT_STR_LEN];
    const char *paraNames[2], *paraValues[2];
    int strlength;

    memset(repStr, 0, SHORT_STR_LEN);
    if (replication > 0) {
        strlength = snprintf(repStr, SHORT_STR_LEN, "%u", replication);
        if (strlength < 0 || strlength >= SHORT_STR_LEN) {
            return EIO;
        }
    }
    paraNames[0] = "replication";
    paraNames[1] = "user.name";
    paraValues[0] = repStr;
    paraValues[1] = user;
    
    return createQueryURL(host, nnPort, path, "SETREPLICATION",
                          2, paraNames, paraValues, url);
}

int createUrlForGetBlockLocations(const char *host, int nnPort,
                                  const char *path, size_t offset,
                                  size_t length, const char *user, char **url)
{
    char offsetStr[LONG_STR_LEN], lengthStr[LONG_STR_LEN];
    const char *paraNames[3], *paraValues[3];
    int strlength;
    
    memset(offsetStr, 0, LONG_STR_LEN);
    memset(lengthStr, 0, LONG_STR_LEN);
    if (offset > 0) {
        strlength = snprintf(offsetStr, LONG_STR_LEN, "%lu", offset);
        if (strlength < 0 || strlength >= LONG_STR_LEN) {
            return EIO;
        }
    }
    if (length > 0) {
        strlength = snprintf(lengthStr, LONG_STR_LEN, "%lu", length);
        if (strlength < 0 || strlength >= LONG_STR_LEN) {
            return EIO;
        }
    }
    paraNames[0] = "offset";
    paraNames[1] = "length";
    paraNames[2] = "user.name";
    paraValues[0] = offsetStr;
    paraValues[1] = lengthStr;
    paraValues[2] = user;
    
    return createQueryURL(host, nnPort, path, "GET_BLOCK_LOCATIONS",
                          3, paraNames, paraValues, url);
}

int createUrlForReadFromDatanode(const char *dnHost, int dnPort,
                                 const char *path, size_t offset,
                                 size_t length, const char *user,
                                 const char *namenodeRpcAddr, char **url)
{
    char offsetStr[LONG_STR_LEN], lengthStr[LONG_STR_LEN];
    const char *paraNames[4], *paraValues[4];
    int strlength;
    
    memset(offsetStr, 0, LONG_STR_LEN);
    memset(lengthStr, 0, LONG_STR_LEN);
    if (offset > 0) {
        strlength = snprintf(offsetStr, LONG_STR_LEN, "%lu", offset);
        if (strlength < 0 || strlength >= LONG_STR_LEN) {
            return EIO;
        }
    }
    if (length > 0) {
        strlength = snprintf(lengthStr, LONG_STR_LEN, "%lu", length);
        if (strlength < 0 || strlength >= LONG_STR_LEN) {
            return EIO;
        }
    }
    
    paraNames[0] = "offset";
    paraNames[1] = "length";
    paraNames[2] = "user.name";
    paraNames[3] = "namenoderpcaddress";
    paraValues[0] = offsetStr;
    paraValues[1] = lengthStr;
    paraValues[2] = user;
    paraValues[3] = namenodeRpcAddr;
    
    return createQueryURL(dnHost, dnPort, path, "OPEN",
                          4, paraNames, paraValues, url);
}