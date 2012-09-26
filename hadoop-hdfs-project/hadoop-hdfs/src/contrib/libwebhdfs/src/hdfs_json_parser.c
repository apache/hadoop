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
#include <ctype.h>
#include <jansson.h>
#include "hdfs_json_parser.h"
#include "exception.h"

hdfsFileInfo *parseJsonGFS(json_t *jobj, hdfsFileInfo *fileStat, int *numEntries, const char *operation); //Forward Declaration

static hdfsFileInfo *json_parse_array(json_t *jobj, char *key, hdfsFileInfo *fileStat, int *numEntries, const char *operation) {
    int arraylen = json_array_size(jobj);                      //Getting the length of the array
    *numEntries = arraylen;
    if (!key) {
        return NULL;
    }
    if(arraylen > 0) {
        fileStat = (hdfsFileInfo *)realloc(fileStat,sizeof(hdfsFileInfo)*arraylen);
    }
    json_t *jvalue;
    int i;
    for (i=0; i< arraylen; i++) {
        jvalue = json_array_get(jobj, i);            //Getting the array element at position i
        if (json_is_array(jvalue)) {                 // array within an array - program should never come here for now
            json_parse_array(jvalue, NULL, &fileStat[i], numEntries, operation);
        }
        else if (json_is_object(jvalue)) {           // program will definitely come over here
            parseJsonGFS(jvalue, &fileStat[i], numEntries, operation);
        }
        else {
            return NULL;                               // program will never come over here for now
        }
    }
    *numEntries = arraylen;
    return fileStat;
}

int parseBoolean(char *response) {
    json_t *root;
    json_error_t error;
    size_t flags = 0;
    int result = 0;
    const char *key;
    json_t *value;
    root = json_loads(response, flags, &error);
    void *iter = json_object_iter(root);
    while(iter)  {
        key = json_object_iter_key(iter);
        value = json_object_iter_value(iter);
        switch (json_typeof(value))  {
            case JSON_TRUE:
                result = 1;
                break;
            default:
                result = 0;
                break;
        }
        iter = json_object_iter_next(root, iter);
    }
    return result;
}

int parseMKDIR(char *response) {
    return (parseBoolean(response));
}

int parseRENAME(char *response) {
    return (parseBoolean(response));
}

int parseDELETE(char *response) {
    return (parseBoolean(response));
}

hdfs_exception_msg *parseJsonException(json_t *jobj) {
    const char *key;
    json_t *value;
    hdfs_exception_msg *exception = NULL;
    
    exception = (hdfs_exception_msg *) calloc(1, sizeof(hdfs_exception_msg));
    if (!exception) {
        return NULL;
    }
    
    void *iter = json_object_iter(jobj);
    while (iter) {
        key = json_object_iter_key(iter);
        value = json_object_iter_value(iter);
        
        if (!strcmp(key, "exception")) {
            exception->exception = json_string_value(value);
        } else if (!strcmp(key, "javaClassName")) {
            exception->javaClassName = json_string_value(value);
        } else if (!strcmp(key, "message")) {
            exception->message = json_string_value(value);
        }
        
        iter = json_object_iter_next(jobj, iter);
    }
    
    return exception;
}

hdfs_exception_msg *parseException(const char *content) {
    if (!content) {
        return NULL;
    }
    
    json_error_t error;
    size_t flags = 0;
    const char *key;
    json_t *value;
    json_t *jobj = json_loads(content, flags, &error);
    
    if (!jobj) {
        fprintf(stderr, "JSon parsing failed\n");
        return NULL;
    }
    void *iter = json_object_iter(jobj);
    while(iter)  {
        key = json_object_iter_key(iter);
        value = json_object_iter_value(iter);
        
        if (!strcmp(key, "RemoteException") && json_typeof(value) == JSON_OBJECT) {
            return parseJsonException(value);
        }
        iter = json_object_iter_next(jobj, iter);
    }
    return NULL;
}

hdfsFileInfo *parseJsonGFS(json_t *jobj, hdfsFileInfo *fileStat, int *numEntries, const char *operation) {
    const char *tempstr;
    const char *key;
    json_t *value;
    void *iter = json_object_iter(jobj);
    while(iter)  {
        key = json_object_iter_key(iter);
        value = json_object_iter_value(iter);
        
        switch (json_typeof(value)) {
            case JSON_INTEGER:
                if(!strcmp(key,"accessTime")) {
                    fileStat->mLastAccess = (time_t)(json_integer_value(value)/1000);
                } else if (!strcmp(key,"blockSize")) {
                    fileStat->mBlockSize = (tOffset)json_integer_value(value);
                } else if (!strcmp(key,"length")) {
                    fileStat->mSize = (tOffset)json_integer_value(value);
                } else if(!strcmp(key,"modificationTime")) {
                    fileStat->mLastMod = (time_t)(json_integer_value(value)/1000);
                } else if (!strcmp(key,"replication")) {
                    fileStat->mReplication = (short)json_integer_value(value);
                }
                break;
                
            case JSON_STRING:
                if(!strcmp(key,"group")) {
                    fileStat->mGroup=(char *)json_string_value(value);
                } else if (!strcmp(key,"owner")) {
                    fileStat->mOwner=(char *)json_string_value(value);
                } else if (!strcmp(key,"pathSuffix")) {
                    fileStat->mName=(char *)json_string_value(value);
                } else if (!strcmp(key,"permission")) {
                    tempstr=(char *)json_string_value(value);
                    fileStat->mPermissions = (short)strtol(tempstr,(char **)NULL,8);
                } else if (!strcmp(key,"type")) {
                    char *cvalue = (char *)json_string_value(value);
                    if (!strcmp(cvalue, "DIRECTORY")) {
                        fileStat->mKind = kObjectKindDirectory;
                    } else {
                        fileStat->mKind = kObjectKindFile;
                    }
                }
                break;
                
            case JSON_OBJECT:
                if(!strcmp(key,"FileStatus")) {
                    parseJsonGFS(value, fileStat, numEntries, operation);
                } else if (!strcmp(key,"FileStatuses")) {
                    fileStat = parseJsonGFS(value, &fileStat[0], numEntries, operation);
                } else if (!strcmp(key,"RemoteException")) {
                    //Besides returning NULL, we also need to print the exception information
                    hdfs_exception_msg *exception = parseJsonException(value);
                    if (exception) {
                        errno = printExceptionWeb(exception, PRINT_EXC_ALL, "Calling WEBHDFS (%s)", operation);
                    }
                    
                    if(fileStat != NULL) {
                        free(fileStat);
                        fileStat = NULL;
                    }
                }
                break;
                
            case JSON_ARRAY:
                if (!strcmp(key,"FileStatus")) {
                    fileStat = json_parse_array(value,(char *) key,fileStat,numEntries, operation);
                }
                break;
                
            default:
                if(fileStat != NULL) {
                    free(fileStat);
                    fileStat = NULL;
                }
        }
        iter = json_object_iter_next(jobj, iter);
    }
    return fileStat;
}


int checkHeader(char *header, const char *content, const char *operation) {
    char *result = NULL;
    char delims[] = ":";
    char *responseCode= "200 OK";
    if(header == '\0' || strncmp(header, "HTTP/", strlen("HTTP/"))) {
        return 0;
    }
    if(!(strstr(header, responseCode)) || !(header = strstr(header, "Content-Length"))) {
        hdfs_exception_msg *exc = parseException(content);
        if (exc) {
            errno = printExceptionWeb(exc, PRINT_EXC_ALL, "Calling WEBHDFS (%s)", operation);
        }
        return 0;
    }
    result = strtok(header, delims);
    result = strtok(NULL,delims);
    while (isspace(*result)) {
        result++;
    }
    if(strcmp(result,"0")) {                 //Content-Length should be equal to 0
        return 1;
    } else {
        return 0;
    }
}

int parseOPEN(const char *header, const char *content) {
    const char *responseCode1 = "307 TEMPORARY_REDIRECT";
    const char *responseCode2 = "200 OK";
    if(header == '\0' || strncmp(header,"HTTP/",strlen("HTTP/"))) {
        return -1;
    }
    if(!(strstr(header,responseCode1) && strstr(header, responseCode2))) {
        hdfs_exception_msg *exc = parseException(content);
        if (exc) {
            //if the exception is an IOException and it is because the offset is out of the range
            //do not print out the exception
            if (!strcasecmp(exc->exception, "IOException") && strstr(exc->message, "out of the range")) {
                return 0;
            }
            errno = printExceptionWeb(exc, PRINT_EXC_ALL, "Calling WEBHDFS (OPEN)");
        }
        return -1;
    }
    
    return 1;
}

int parseCHMOD(char *header, const char *content) {
    return checkHeader(header, content, "CHMOD");
}


int parseCHOWN(char *header, const char *content) {
    return checkHeader(header, content, "CHOWN");
}

int parseUTIMES(char *header, const char *content) {
    return checkHeader(header, content, "UTIMES");
}


int checkIfRedirect(const char *const headerstr, const char *content, const char *operation) {
    char *responseCode = "307 TEMPORARY_REDIRECT";
    char * locTag = "Location";
    char * tempHeader;
    if(headerstr == '\0' || strncmp(headerstr,"HTTP/", 5)) {
        return 0;
    }
    if(!(tempHeader = strstr(headerstr,responseCode))) {
        //process possible exception information
        hdfs_exception_msg *exc = parseException(content);
        if (exc) {
            errno = printExceptionWeb(exc, PRINT_EXC_ALL, "Calling WEBHDFS (%s)", operation);
        }
        return 0;
    }
    if(!(strstr(tempHeader,locTag))) {
        return 0;
    }
    return 1;
}


int parseNnWRITE(const char *header, const char *content) {
    return checkIfRedirect(header, content, "Write(NameNode)");
}


int parseNnAPPEND(const char *header, const char *content) {
    return checkIfRedirect(header, content, "Append(NameNode)");
}

char *parseDnLoc(char *content) {
    char delims[] = "\r\n";
    char *url = NULL;
    char *DnLocation = NULL;
    char *savepter;
    DnLocation = strtok_r(content, delims, &savepter);
    while (DnLocation && strncmp(DnLocation, "Location:", strlen("Location:"))) {
        DnLocation = strtok_r(NULL, delims, &savepter);
    }
    if (!DnLocation) {
        return NULL;
    }
    DnLocation = strstr(DnLocation, "http");
    if (!DnLocation) {
        return NULL;
    }
    url = malloc(strlen(DnLocation) + 1);
    if (!url) {
        return NULL;
    }
    strcpy(url, DnLocation);
    return url;
}

int parseDnWRITE(const char *header, const char *content) {
    char *responseCode = "201 Created";
    fprintf(stderr, "\nheaderstr is: %s\n", header);
    if(header == '\0' || strncmp(header,"HTTP/",strlen("HTTP/"))) {
        return 0;
    }
    if(!(strstr(header,responseCode))) {
        hdfs_exception_msg *exc = parseException(content);
        if (exc) {
            errno = printExceptionWeb(exc, PRINT_EXC_ALL, "Calling WEBHDFS (WRITE(DataNode))");
        }
        return 0;
    }
    return 1;
}

int parseDnAPPEND(const char *header, const char *content) {
    char *responseCode = "200 OK";
    if(header == '\0' || strncmp(header, "HTTP/", strlen("HTTP/"))) {
        return 0;
    }
    if(!(strstr(header, responseCode))) {
        hdfs_exception_msg *exc = parseException(content);
        if (exc) {
            errno = printExceptionWeb(exc, PRINT_EXC_ALL, "Calling WEBHDFS (APPEND(DataNode))");
        }
        return 0;
    }
    return 1;
}

hdfsFileInfo *parseGFS(char *str, hdfsFileInfo *fileStat, int *numEntries) {
    json_error_t error;
    size_t flags = 0;
    json_t *jobj = json_loads(str, flags, &error);
    fileStat = parseJsonGFS(jobj, fileStat, numEntries, "GETPATHSTATUS/LISTSTATUS");
    return fileStat;
}

int parseSETREPLICATION(char *response) {
    return (parseBoolean(response));
}

