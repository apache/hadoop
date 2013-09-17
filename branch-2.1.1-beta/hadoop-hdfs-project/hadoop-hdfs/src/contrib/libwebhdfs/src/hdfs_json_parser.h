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
#ifndef _HDFS_JSON_PARSER_H_
#define _HDFS_JSON_PARSER_H_

/**
 * Parse the response for MKDIR request. The response uses TRUE/FALSE 
 * to indicate whether the operation succeeded.
 *
 * @param response  The response information to parse.
 * @return 0 for success
 */
int parseMKDIR(const char *response);

/**
 * Parse the response for RENAME request. The response uses TRUE/FALSE
 * to indicate whether the operation succeeded.
 *
 * @param response  The response information to parse.
 * @return 0 for success
 */
int parseRENAME(const char *response);

/**
 * Parse the response for DELETE request. The response uses TRUE/FALSE
 * to indicate whether the operation succeeded.
 *
 * @param response  The response information to parse.
 * @return 0 for success
 */
int parseDELETE(const char *response);

/**
 * Parse the response for SETREPLICATION request. The response uses TRUE/FALSE
 * to indicate whether the operation succeeded.
 *
 * @param response  The response information to parse.
 * @return 0 for success
 */
int parseSETREPLICATION(const char *response);

/**
 * Parse the response for OPEN (read) request. A successful operation 
 * will return "200 OK".
 *
 * @param response  The response information for parsing
 * @return          0 for success , -1 for out of range, other values for error
 */
int parseOPEN(const char *header, const char *content);

/**
 * Parse the response for WRITE (from NameNode) request. 
 * A successful operation should return "307 TEMPORARY_REDIRECT" in its header.
 *
 * @param header    The header of the http response
 * @param content   If failing, the exception message 
 *                  sent from NameNode is stored in content
 * @return          0 for success
 */
int parseNnWRITE(const char *header, const char *content);

/**
 * Parse the response for WRITE (from DataNode) request. 
 * A successful operation should return "201 Created" in its header.
 * 
 * @param header    The header of the http response
 * @param content   If failing, the exception message
 *                  sent from DataNode is stored in content
 * @return          0 for success
 */
int parseDnWRITE(const char *header, const char *content);

/**
 * Parse the response for APPEND (sent from NameNode) request.
 * A successful operation should return "307 TEMPORARY_REDIRECT" in its header.
 *
 * @param header    The header of the http response
 * @param content   If failing, the exception message
 *                  sent from NameNode is stored in content
 * @return          0 for success
 */
int parseNnAPPEND(const char *header, const char *content);

/**
 * Parse the response for APPEND (from DataNode) request.
 * A successful operation should return "200 OK" in its header.
 *
 * @param header    The header of the http response
 * @param content   If failing, the exception message
 *                  sent from DataNode is stored in content
 * @return          0 for success
 */
int parseDnAPPEND(const char *header, const char *content);

/**
 * Parse the response (from NameNode) to get the location information 
 * of the DataNode that should be contacted for the following write operation.
 *
 * @param content   Content of the http header
 * @param dn        To store the location of the DataNode for writing
 * @return          0 for success
 */
int parseDnLoc(char *content, char **dn) __attribute__ ((warn_unused_result));

/**
 * Parse the response for GETFILESTATUS operation.
 *
 * @param response      Response to parse. Its detailed format is specified in 
 *            "http://hadoop.apache.org/docs/stable/webhdfs.html#GETFILESTATUS"
 * @param fileStat      A hdfsFileInfo handle for holding file information
 * @param printError    Whether or not print out exception 
 *                      when file does not exist
 * @return 0 for success, non-zero value to indicate error
 */
int parseGFS(const char *response, hdfsFileInfo *fileStat, int printError);

/**
 * Parse the response for LISTSTATUS operation.
 *
 * @param response      Response to parse. Its detailed format is specified in
 *            "http://hadoop.apache.org/docs/r1.0.3/webhdfs.html#LISTSTATUS"
 * @param fileStats     Pointer pointing to a list of hdfsFileInfo handles 
 *                      holding file/dir information in the directory
 * @param numEntries    After parsing, the value of this parameter indicates
 *                      the number of file entries.
 * @return 0 for success, non-zero value to indicate error
 */
int parseLS(const char *response, hdfsFileInfo **fileStats, int *numOfEntries);

/**
 * Parse the response for CHOWN request.
 * A successful operation should contains "200 OK" in its header, 
 * and the Content-Length should be 0.
 *
 * @param header    The header of the http response
 * @param content   If failing, the exception message is stored in content
 * @return          0 for success
 */
int parseCHOWN(const char *header, const char *content);

/**
 * Parse the response for CHMOD request.
 * A successful operation should contains "200 OK" in its header,
 * and the Content-Length should be 0.
 *
 * @param header    The header of the http response
 * @param content   If failing, the exception message is stored in content
 * @return          0 for success
 */
int parseCHMOD(const char *header, const char *content);

/**
 * Parse the response for SETTIMES request.
 * A successful operation should contains "200 OK" in its header,
 * and the Content-Length should be 0.
 *
 * @param header    The header of the http response
 * @param content   If failing, the exception message is stored in content
 * @return          0 for success
 */
int parseUTIMES(const char *header, const char *content);

#endif //_HDFS_JSON_PARSER_H_
