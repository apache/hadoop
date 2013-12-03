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
#include "hdfs.h" /* for hdfsFileInfo */
#include "hdfs_json_parser.h"

#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <jansson.h>

static const char * const temporaryRedirectCode = "307 TEMPORARY_REDIRECT";
static const char * const twoHundredOKCode = "200 OK";
static const char * const twoHundredOneCreatedCode = "201 Created";
static const char * const httpHeaderString = "HTTP/1.1";

/**
 * Exception information after calling JSON operations
 */
struct jsonException {
  const char *exception;
  const char *javaClassName;
  const char *message;
};

/** Print out the JSON exception information */
static int printJsonExceptionV(struct jsonException *exc, int noPrintFlags,
                               const char *fmt, va_list ap)
{
    char *javaClassName = NULL;
    int excErrno = EINTERNAL, shouldPrint = 0;
    if (!exc) {
        fprintf(stderr, "printJsonExceptionV: the jsonException is NULL\n");
        return EINTERNAL;
    }
    javaClassName = strdup(exc->javaClassName);
    if (!javaClassName) {
        fprintf(stderr, "printJsonExceptionV: internal out of memory error\n");
        return EINTERNAL;
    }
    getExceptionInfo(javaClassName, noPrintFlags, &excErrno, &shouldPrint);
    free(javaClassName);
    
    if (shouldPrint) {
        vfprintf(stderr, fmt, ap);
        fprintf(stderr, " error:\n");
        fprintf(stderr, "Exception: %s\nJavaClassName: %s\nMessage: %s\n",
                exc->exception, exc->javaClassName, exc->message);
    }
    
    free(exc);
    return excErrno;
}

/**
 * Print out JSON exception information.
 *
 * @param exc             The exception information to print and free
 * @param noPrintFlags    Flags which determine which exceptions we should NOT
 *                        print.
 * @param fmt             Printf-style format list
 * @param ...             Printf-style varargs
 *
 * @return                The POSIX error number associated with the exception
 *                        object.
 */
static int printJsonException(struct jsonException *exc, int noPrintFlags,
                              const char *fmt, ...)
{
    va_list ap;
    int ret = 0;
    
    va_start(ap, fmt);
    ret = printJsonExceptionV(exc, noPrintFlags, fmt, ap);
    va_end(ap);
    return ret;
}

/** Parse the exception information from JSON */
static struct jsonException *parseJsonException(json_t *jobj)
{
    const char *key = NULL;
    json_t *value = NULL;
    struct jsonException *exception = NULL;
    void *iter = NULL;
    
    exception = calloc(1, sizeof(*exception));
    if (!exception) {
        return NULL;
    }
    
    iter = json_object_iter(jobj);
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

/** 
 * Parse the exception information which is presented in JSON
 * 
 * @param content   Exception information in JSON
 * @return          jsonException for printing out
 */
static struct jsonException *parseException(const char *content)
{
    json_error_t error;
    size_t flags = 0;
    const char *key = NULL;
    json_t *value;
    json_t *jobj;
    struct jsonException *exception = NULL;
    
    if (!content) {
        return NULL;
    }
    jobj = json_loads(content, flags, &error);
    if (!jobj) {
        fprintf(stderr, "JSon parsing error: on line %d: %s\n",
                error.line, error.text);
        return NULL;
    }
    void *iter = json_object_iter(jobj);
    while(iter)  {
        key = json_object_iter_key(iter);
        value = json_object_iter_value(iter);
        
        if (!strcmp(key, "RemoteException") &&
                    json_typeof(value) == JSON_OBJECT) {
            exception = parseJsonException(value);
            break;
        }
        iter = json_object_iter_next(jobj, iter);
    }
    
    json_decref(jobj);
    return exception;
}

/**
 * Parse the response information which uses TRUE/FALSE 
 * to indicate whether the operation succeeded
 *
 * @param response  Response information
 * @return          0 to indicate success
 */
static int parseBoolean(const char *response)
{
    json_t *root, *value;
    json_error_t error;
    size_t flags = 0;
    int result = 0;
    
    root = json_loads(response, flags, &error);
    if (!root) {
        fprintf(stderr, "JSon parsing error: on line %d: %s\n",
                error.line, error.text);
        return EIO;
    }
    void *iter = json_object_iter(root);
    value = json_object_iter_value(iter);
    if (json_typeof(value) == JSON_TRUE)  {
        result = 0;
    } else {
        result = EIO;  // FALSE means error in remote NN/DN
    }
    json_decref(root);
    return result;
}

int parseMKDIR(const char *response)
{
    return parseBoolean(response);
}

int parseRENAME(const char *response)
{
    return parseBoolean(response);
}

int parseDELETE(const char *response)
{
    return parseBoolean(response);
}

int parseSETREPLICATION(const char *response)
{
    return parseBoolean(response);
}

/**
 * Check the header of response to see if it's 200 OK
 * 
 * @param header    Header information for checking
 * @param content   Stores exception information if there are errors
 * @param operation Indicate the operation for exception printing
 * @return 0 for success
 */
static int checkHeader(const char *header, const char *content,
                       const char *operation)
{
    char *result = NULL;
    const char delims[] = ":";
    char *savepter;
    int ret = 0;
    
    if (!header || strncmp(header, "HTTP/", strlen("HTTP/"))) {
        return EINVAL;
    }
    if (!(strstr(header, twoHundredOKCode)) ||
       !(result = strstr(header, "Content-Length"))) {
        struct jsonException *exc = parseException(content);
        if (exc) {
            ret = printJsonException(exc, PRINT_EXC_ALL,
                                       "Calling WEBHDFS (%s)", operation);
        } else {
            ret = EIO;
        }
        return ret;
    }
    result = strtok_r(result, delims, &savepter);
    result = strtok_r(NULL, delims, &savepter);
    while (isspace(*result)) {
        result++;
    }
    // Content-Length should be equal to 0,
    // and the string should be "0\r\nServer"
    if (strncmp(result, "0\r\n", 3)) {
        ret = EIO;
    }
    return ret;
}

int parseCHMOD(const char *header, const char *content)
{
    return checkHeader(header, content, "CHMOD");
}

int parseCHOWN(const char *header, const char *content)
{
    return checkHeader(header, content, "CHOWN");
}

int parseUTIMES(const char *header, const char *content)
{
    return checkHeader(header, content, "SETTIMES");
}

/**
 * Check if the header contains correct information
 * ("307 TEMPORARY_REDIRECT" and "Location")
 * 
 * @param header    Header for parsing
 * @param content   Contains exception information 
 *                  if the remote operation failed
 * @param operation Specify the remote operation when printing out exception
 * @return 0 for success
 */
static int checkRedirect(const char *header,
                         const char *content, const char *operation)
{
    const char *locTag = "Location";
    int ret = 0, offset = 0;
    
    // The header must start with "HTTP/1.1"
    if (!header || strncmp(header, httpHeaderString,
                           strlen(httpHeaderString))) {
        return EINVAL;
    }
    
    offset += strlen(httpHeaderString);
    while (isspace(header[offset])) {
        offset++;
    }
    // Looking for "307 TEMPORARY_REDIRECT" in header
    if (strncmp(header + offset, temporaryRedirectCode,
                strlen(temporaryRedirectCode))) {
        // Process possible exception information
        struct jsonException *exc = parseException(content);
        if (exc) {
            ret = printJsonException(exc, PRINT_EXC_ALL,
                                     "Calling WEBHDFS (%s)", operation);
        } else {
            ret = EIO;
        }
        return ret;
    }
    // Here we just simply check if header contains "Location" tag,
    // detailed processing is in parseDnLoc
    if (!(strstr(header, locTag))) {
        ret = EIO;
    }
    return ret;
}

int parseNnWRITE(const char *header, const char *content)
{
    return checkRedirect(header, content, "Write(NameNode)");
}

int parseNnAPPEND(const char *header, const char *content)
{
    return checkRedirect(header, content, "Append(NameNode)");
}

/** 0 for success , -1 for out of range, other values for error */
int parseOPEN(const char *header, const char *content)
{
    int ret = 0, offset = 0;
    
    if (!header || strncmp(header, httpHeaderString,
                           strlen(httpHeaderString))) {
        return EINVAL;
    }
    
    offset += strlen(httpHeaderString);
    while (isspace(header[offset])) {
        offset++;
    }
    if (strncmp(header + offset, temporaryRedirectCode,
                strlen(temporaryRedirectCode)) ||
        !strstr(header, twoHundredOKCode)) {
        struct jsonException *exc = parseException(content);
        if (exc) {
            // If the exception is an IOException and it is because
            // the offset is out of the range, do not print out the exception
            if (!strcasecmp(exc->exception, "IOException") &&
                    strstr(exc->message, "out of the range")) {
                ret = -1;
            } else {
                ret = printJsonException(exc, PRINT_EXC_ALL,
                                       "Calling WEBHDFS (OPEN)");
            }
        } else {
            ret = EIO;
        }
    }
    return ret;
}

int parseDnLoc(char *content, char **dn)
{
    char *url = NULL, *dnLocation = NULL, *savepter, *tempContent;
    const char *prefix = "Location: http://";
    const char *prefixToRemove = "Location: ";
    const char *delims = "\r\n";
    
    tempContent = strdup(content);
    if (!tempContent) {
        return ENOMEM;
    }
    
    dnLocation = strtok_r(tempContent, delims, &savepter);
    while (dnLocation && strncmp(dnLocation, "Location:",
                                 strlen("Location:"))) {
        dnLocation = strtok_r(NULL, delims, &savepter);
    }
    if (!dnLocation) {
        return EIO;
    }
    
    while (isspace(*dnLocation)) {
        dnLocation++;
    }
    if (strncmp(dnLocation, prefix, strlen(prefix))) {
        return EIO;
    }
    url = strdup(dnLocation + strlen(prefixToRemove));
    if (!url) {
        return ENOMEM;
    }
    *dn = url;
    return 0;
}

int parseDnWRITE(const char *header, const char *content)
{
    int ret = 0;
    if (header == NULL || header[0] == '\0' ||
                         strncmp(header, "HTTP/", strlen("HTTP/"))) {
        return EINVAL;
    }
    if (!(strstr(header, twoHundredOneCreatedCode))) {
        struct jsonException *exc = parseException(content);
        if (exc) {
            ret = printJsonException(exc, PRINT_EXC_ALL,
                                     "Calling WEBHDFS (WRITE(DataNode))");
        } else {
            ret = EIO;
        }
    }
    return ret;
}

int parseDnAPPEND(const char *header, const char *content)
{
    int ret = 0;
    
    if (header == NULL || header[0] == '\0' ||
                         strncmp(header, "HTTP/", strlen("HTTP/"))) {
        return EINVAL;
    }
    if (!(strstr(header, twoHundredOKCode))) {
        struct jsonException *exc = parseException(content);
        if (exc) {
            ret = printJsonException(exc, PRINT_EXC_ALL,
                                     "Calling WEBHDFS (APPEND(DataNode))");
        } else {
            ret = EIO;
        }
    }
    return ret;
}

/**
 * Retrieve file status from the JSON object 
 *
 * @param jobj          JSON object for parsing, which contains 
 *                      file status information
 * @param fileStat      hdfsFileInfo handle to hold file status information
 * @return 0 on success
 */
static int parseJsonForFileStatus(json_t *jobj, hdfsFileInfo *fileStat)
{
    const char *key, *tempstr;
    json_t *value;
    void *iter = NULL;
    
    iter = json_object_iter(jobj);
    while (iter) {
        key = json_object_iter_key(iter);
        value = json_object_iter_value(iter);
        
        if (!strcmp(key, "accessTime")) {
            // json field contains time in milliseconds,
            // hdfsFileInfo is counted in seconds
            fileStat->mLastAccess = json_integer_value(value) / 1000;
        } else if (!strcmp(key, "blockSize")) {
            fileStat->mBlockSize = json_integer_value(value);
        } else if (!strcmp(key, "length")) {
            fileStat->mSize = json_integer_value(value);
        } else if (!strcmp(key, "modificationTime")) {
            fileStat->mLastMod = json_integer_value(value) / 1000;
        } else if (!strcmp(key, "replication")) {
            fileStat->mReplication = json_integer_value(value);
        } else if (!strcmp(key, "group")) {
            fileStat->mGroup = strdup(json_string_value(value));
            if (!fileStat->mGroup) {
                return ENOMEM;
            }
        } else if (!strcmp(key, "owner")) {
            fileStat->mOwner = strdup(json_string_value(value));
            if (!fileStat->mOwner) {
                return ENOMEM;
            }
        } else if (!strcmp(key, "pathSuffix")) {
            fileStat->mName = strdup(json_string_value(value));
            if (!fileStat->mName) {
                return ENOMEM;
            }
        } else if (!strcmp(key, "permission")) {
            tempstr = json_string_value(value);
            fileStat->mPermissions = (short) strtol(tempstr, NULL, 8);
        } else if (!strcmp(key, "type")) {
            tempstr = json_string_value(value);
            if (!strcmp(tempstr, "DIRECTORY")) {
                fileStat->mKind = kObjectKindDirectory;
            } else {
                fileStat->mKind = kObjectKindFile;
            }
        }
        // Go to the next key-value pair in the json object
        iter = json_object_iter_next(jobj, iter);
    }
    return 0;
}

int parseGFS(const char *response, hdfsFileInfo *fileStat, int printError)
{
    int ret = 0, printFlag;
    json_error_t error;
    size_t flags = 0;
    json_t *jobj, *value;
    const char *key;
    void *iter = NULL;
    
    if (!response || !fileStat) {
        return EIO;
    }
    jobj = json_loads(response, flags, &error);
    if (!jobj) {
        fprintf(stderr, "error while parsing json: on line %d: %s\n",
                error.line, error.text);
        return EIO;
    }
    iter = json_object_iter(jobj);
    key = json_object_iter_key(iter);
    value = json_object_iter_value(iter);
    if (json_typeof(value) == JSON_OBJECT) {
        if (!strcmp(key, "RemoteException")) {
            struct jsonException *exception = parseJsonException(value);
            if (exception) {
                if (printError) {
                    printFlag = PRINT_EXC_ALL;
                } else {
                    printFlag = NOPRINT_EXC_FILE_NOT_FOUND |
                                NOPRINT_EXC_ACCESS_CONTROL |
                                NOPRINT_EXC_PARENT_NOT_DIRECTORY;
                }
                ret = printJsonException(exception, printFlag,
                                         "Calling WEBHDFS GETFILESTATUS");
            } else {
                ret = EIO;
            }
        } else if (!strcmp(key, "FileStatus")) {
            ret = parseJsonForFileStatus(value, fileStat);
        } else {
            ret = EIO;
        }
        
    } else {
        ret = EIO;
    }
    
    json_decref(jobj);
    return ret;
}

/**
 * Parse the JSON array. Called to parse the result of 
 * the LISTSTATUS operation. Thus each element of the JSON array is 
 * a JSON object with the information of a file entry contained 
 * in the folder.
 *
 * @param jobj          The JSON array to be parsed
 * @param fileStat      The hdfsFileInfo handle used to 
 *                      store a group of file information
 * @param numEntries    Capture the number of files in the folder
 * @return              0 for success
 */
static int parseJsonArrayForFileStatuses(json_t *jobj, hdfsFileInfo **fileStat,
                                         int *numEntries)
{
    json_t *jvalue = NULL;
    int i = 0, ret = 0, arraylen = 0;
    hdfsFileInfo *fileInfo = NULL;
    
    arraylen = (int) json_array_size(jobj);
    if (arraylen > 0) {
        fileInfo = calloc(arraylen, sizeof(hdfsFileInfo));
        if (!fileInfo) {
            return ENOMEM;
        }
    }
    for (i = 0; i < arraylen; i++) {
        //Getting the array element at position i
        jvalue = json_array_get(jobj, i);
        if (json_is_object(jvalue)) {
            ret = parseJsonForFileStatus(jvalue, &fileInfo[i]);
            if (ret) {
                goto done;
            }
        } else {
            ret = EIO;
            goto done;
        }
    }
done:
    if (ret) {
        free(fileInfo);
    } else {
        *numEntries = arraylen;
        *fileStat = fileInfo;
    }
    return ret;
}

int parseLS(const char *response, hdfsFileInfo **fileStats, int *numOfEntries)
{
    int ret = 0;
    json_error_t error;
    size_t flags = 0;
    json_t *jobj, *value;
    const char *key;
    void *iter = NULL;
    
    if (!response || response[0] == '\0' || !fileStats) {
        return EIO;
    }
    jobj = json_loads(response, flags, &error);
    if (!jobj) {
        fprintf(stderr, "error while parsing json: on line %d: %s\n",
                error.line, error.text);
        return EIO;
    }
    
    iter = json_object_iter(jobj);
    key = json_object_iter_key(iter);
    value = json_object_iter_value(iter);
    if (json_typeof(value) == JSON_OBJECT) {
        if (!strcmp(key, "RemoteException")) {
            struct jsonException *exception = parseJsonException(value);
            if (exception) {
                ret = printJsonException(exception, PRINT_EXC_ALL,
                                         "Calling WEBHDFS GETFILESTATUS");
            } else {
                ret = EIO;
            }
        } else if (!strcmp(key, "FileStatuses")) {
            iter = json_object_iter(value);
            value = json_object_iter_value(iter);
            if (json_is_array(value)) {
                ret = parseJsonArrayForFileStatuses(value, fileStats,
                                                    numOfEntries);
            } else {
                ret = EIO;
            }
        } else {
            ret = EIO;
        }
    } else {
        ret = EIO;
    }
    
    json_decref(jobj);
    return ret;
}
