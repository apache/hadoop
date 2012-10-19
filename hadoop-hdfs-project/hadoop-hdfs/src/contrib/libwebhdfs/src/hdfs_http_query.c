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

#define NUM_OF_PERMISSION_BITS 4
#define NUM_OF_PORT_BITS 6
#define NUM_OF_REPLICATION_BITS 6

static char *prepareQUERY(const char *host, int nnPort, const char *srcpath, const char *OP, const char *user) {
    size_t length;
    char *url;
    const char *const protocol = "http://";
    const char *const prefix = "/webhdfs/v1";
    char *temp;
    char *port;
    port= (char*) malloc(NUM_OF_PORT_BITS);
    if (!port) {
        return NULL;
    }
    sprintf(port,"%d",nnPort);
    if (user != NULL) {
        length = strlen(protocol) + strlen(host) + strlen(":") + strlen(port) + strlen(prefix) + strlen(srcpath) + strlen ("?op=") + strlen(OP) + strlen("&user.name=") + strlen(user);
    } else {
        length = strlen(protocol) + strlen(host) + strlen(":") + strlen(port) + strlen(prefix) + strlen(srcpath) +  strlen ("?op=") + strlen(OP);
    }
    
    temp = (char*) malloc(length + 1);
    if (!temp) {
        return NULL;
    }
    strcpy(temp,protocol);
    temp = strcat(temp,host);
    temp = strcat(temp,":");
    temp = strcat(temp,port);
    temp = strcat(temp,prefix);
    temp = strcat(temp,srcpath);
    temp = strcat(temp,"?op=");
    temp = strcat(temp,OP);
    if (user) {
        temp = strcat(temp,"&user.name=");
        temp = strcat(temp,user);
    }
    url = temp;
    return url;
}


static int decToOctal(int decNo) {
    int octNo=0;
    int expo =0;
    while (decNo != 0)  {
        octNo = ((decNo % 8) * pow(10,expo)) + octNo;
        decNo = decNo / 8;
        expo++;
    }
    return octNo;
}


char *prepareMKDIR(const char *host, int nnPort, const char *dirsubpath, const char *user) {
    return prepareQUERY(host, nnPort, dirsubpath, "MKDIRS", user);
}


char *prepareMKDIRwithMode(const char *host, int nnPort, const char *dirsubpath, int mode, const char *user) {
    char *url;
    char *permission;
    permission = (char*) malloc(NUM_OF_PERMISSION_BITS);
    if (!permission) {
        return NULL;
    }
    mode = decToOctal(mode);
    sprintf(permission,"%d",mode);
    url = prepareMKDIR(host, nnPort, dirsubpath, user);
    url = realloc(url,(strlen(url) + strlen("&permission=") + strlen(permission) + 1));
    if (!url) {
        return NULL;
    }
    url = strcat(url,"&permission=");
    url = strcat(url,permission);
    return url;
}


char *prepareRENAME(const char *host, int nnPort, const char *srcpath, const char *destpath, const char *user) {
    char *url;
    url = prepareQUERY(host, nnPort, srcpath, "RENAME", user);
    url = realloc(url,(strlen(url) + strlen("&destination=") + strlen(destpath) + 1));
    if (!url) {
        return NULL;
    }
    url = strcat(url,"&destination=");
    url = strcat(url,destpath);
    return url;
}

char *prepareGFS(const char *host, int nnPort, const char *dirsubpath, const char *user) {
    return (prepareQUERY(host, nnPort, dirsubpath, "GETFILESTATUS", user));
}

char *prepareLS(const char *host, int nnPort, const char *dirsubpath, const char *user) {
    return (prepareQUERY(host, nnPort, dirsubpath, "LISTSTATUS", user));
}

char *prepareCHMOD(const char *host, int nnPort, const char *dirsubpath, int mode, const char *user) {
    char *url;
    char *permission;
    permission = (char*) malloc(NUM_OF_PERMISSION_BITS);
    if (!permission) {
        return NULL;
    }
    mode &= 0x3FFF;
    mode = decToOctal(mode);
    sprintf(permission,"%d",mode);
    url = prepareQUERY(host, nnPort, dirsubpath, "SETPERMISSION", user);
    url = realloc(url,(strlen(url) + strlen("&permission=") + strlen(permission) + 1));
    if (!url) {
        return NULL;
    }
    url = strcat(url,"&permission=");
    url = strcat(url,permission);
    return url;
}

char *prepareDELETE(const char *host, int nnPort, const char *dirsubpath, int recursive, const char *user) {
    char *url = (prepareQUERY(host, nnPort, dirsubpath, "DELETE", user));
    char *recursiveFlag = (char *)malloc(6);
    if (!recursive) {
        strcpy(recursiveFlag, "false");
    } else {
        strcpy(recursiveFlag, "true");
    }
    url = (char *) realloc(url, strlen(url) + strlen("&recursive=") + strlen(recursiveFlag) + 1);
    if (!url) {
        return NULL;
    }
    
    strcat(url, "&recursive=");
    strcat(url, recursiveFlag);
    return url;
}

char *prepareCHOWN(const char *host, int nnPort, const char *dirsubpath, const char *owner, const char *group, const char *user) {
    char *url;
    url = prepareQUERY(host, nnPort, dirsubpath, "SETOWNER", user);
    if (!url) {
        return NULL;
    }
    if(owner != NULL) {
        url = realloc(url,(strlen(url) + strlen("&owner=") + strlen(owner) + 1));
        url = strcat(url,"&owner=");
        url = strcat(url,owner);
    }
    if (group != NULL) {
        url = realloc(url,(strlen(url) + strlen("&group=") + strlen(group) + 1));
        url = strcat(url,"&group=");
        url = strcat(url,group);
    }
    return url;
}

char *prepareOPEN(const char *host, int nnPort, const char *dirsubpath, const char *user, size_t offset, size_t length) {
    char *base_url = prepareQUERY(host, nnPort, dirsubpath, "OPEN", user);
    char *url = (char *) malloc(strlen(base_url) + strlen("&offset=") + 15 + strlen("&length=") + 15);
    if (!url) {
        return NULL;
    }
    sprintf(url, "%s&offset=%ld&length=%ld", base_url, offset, length);
    return url;
}

char *prepareUTIMES(const char *host, int nnPort, const char *dirsubpath, long unsigned mTime, long unsigned aTime, const char *user) {
    char *url;
    char *modTime;
    char *acsTime;
    modTime = (char*) malloc(12);
    acsTime = (char*) malloc(12);
    url = prepareQUERY(host, nnPort, dirsubpath, "SETTIMES", user);
    sprintf(modTime,"%lu",mTime);
    sprintf(acsTime,"%lu",aTime);
    url = realloc(url,(strlen(url) + strlen("&modificationtime=") + strlen(modTime) + strlen("&accesstime=") + strlen(acsTime) + 1));
    if (!url) {
        return NULL;
    }
    url = strcat(url, "&modificationtime=");
    url = strcat(url, modTime);
    url = strcat(url,"&accesstime=");
    url = strcat(url, acsTime);
    return url;
}

char *prepareNnWRITE(const char *host, int nnPort, const char *dirsubpath, const char *user, int16_t replication, size_t blockSize) {
    char *url;
    url = prepareQUERY(host, nnPort, dirsubpath, "CREATE", user);
    url = realloc(url, (strlen(url) + strlen("&overwrite=true") + 1));
    if (!url) {
        return NULL;
    }
    url = strcat(url, "&overwrite=true");
    if (replication > 0) {
        url = realloc(url, (strlen(url) + strlen("&replication=") + 6));
        if (!url) {
            return NULL;
        }
        sprintf(url, "%s&replication=%d", url, replication);
    }
    if (blockSize > 0) {
        url = realloc(url, (strlen(url) + strlen("&blocksize=") + 16));
        if (!url) {
            return NULL;
        }
        sprintf(url, "%s&blocksize=%ld", url, blockSize);
    }
    return url;
}

char *prepareNnAPPEND(const char *host, int nnPort, const char *dirsubpath, const char *user) {
    return (prepareQUERY(host, nnPort, dirsubpath, "APPEND", user));
}

char *prepareSETREPLICATION(const char *host, int nnPort, const char *path, int16_t replication, const char *user)
{
    char *url = prepareQUERY(host, nnPort, path, "SETREPLICATION", user);
    char *replicationNum = (char *) malloc(NUM_OF_REPLICATION_BITS);
    sprintf(replicationNum, "%u", replication);
    url = realloc(url, strlen(url) + strlen("&replication=") + strlen(replicationNum)+ 1);
    if (!url) {
        return NULL;
    }
    
    url = strcat(url, "&replication=");
    url = strcat(url, replicationNum);
    return url;
}