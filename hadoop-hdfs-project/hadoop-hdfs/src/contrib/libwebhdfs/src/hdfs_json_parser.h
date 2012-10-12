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

struct jsonException;

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
int printJsonException(struct jsonException *exc, int noPrintFlags,
                       const char *fmt, ...);

int parseMKDIR(char *response);
int parseRENAME(char *response);
int parseDELETE (char *response);
int parseSETREPLICATION(char *response);

int parseOPEN(const char *header, const char *content);

int parseNnWRITE(const char *header, const char *content);
int parseDnWRITE(const char *header, const char *content);
int parseNnAPPEND(const char *header, const char *content);
int parseDnAPPEND(const char *header, const char *content);

char* parseDnLoc(char *content);

hdfsFileInfo *parseGFS(char *response, hdfsFileInfo *fileStat, int *numEntries);

int parseCHOWN (char *header, const char *content);
int parseCHMOD (char *header, const char *content);
int parseUTIMES(char *header, const char *content);

#endif //_FUSE_JSON_PARSER_H
