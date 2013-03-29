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

#include "hdfs.h"
#include "native_mini_dfs.h"

#include <inttypes.h>
#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static struct NativeMiniDfsCluster *cluster;

void permission_disp(short permissions, char *rtr)
{
    rtr[9] = '\0';
    int i;
    short perm;
    for(i = 2; i >= 0; i--)
    {
        perm = permissions >> (i * 3);
        rtr[0] = perm & 4 ? 'r' : '-';
        rtr[1] = perm & 2 ? 'w' : '-';
        rtr[2] = perm & 1 ? 'x' : '-';
        rtr += 3;
    }
}

int main(int argc, char **argv)
{
    char buffer[32];
    tSize num_written_bytes;
    const char* slashTmp = "/tmp";
    int nnPort;
    char *rwTemplate, *rwTemplate2, *newDirTemplate,
    *appendTemplate, *userTemplate, *rwPath = NULL;
    const char* fileContents = "Hello, World!";
    const char* nnHost = NULL;
    
    if (argc != 2) {
        fprintf(stderr, "usage: test_libwebhdfs_ops <username>\n");
        exit(1);
    }
    
    struct NativeMiniDfsConf conf = {
        .doFormat = 1, .webhdfsEnabled = 1, .namenodeHttpPort = 50070,
    };
    cluster = nmdCreate(&conf);
    if (!cluster) {
        fprintf(stderr, "Failed to create the NativeMiniDfsCluster.\n");
        exit(1);
    }
    if (nmdWaitClusterUp(cluster)) {
        fprintf(stderr, "Error when waiting for cluster to be ready.\n");
        exit(1);
    }
    if (nmdGetNameNodeHttpAddress(cluster, &nnPort, &nnHost)) {
        fprintf(stderr, "Error when retrieving namenode host address.\n");
        exit(1);
    }
    
    hdfsFS fs = hdfsConnectAsUserNewInstance(nnHost, nnPort, argv[1]);
    if(!fs) {
        fprintf(stderr, "Oops! Failed to connect to hdfs!\n");
        exit(-1);
    }
    
    {
        // Write tests
        rwTemplate = strdup("/tmp/helloWorldXXXXXX");
        if (!rwTemplate) {
            fprintf(stderr, "Failed to create rwTemplate!\n");
            exit(1);
        }
        rwPath = mktemp(rwTemplate);
        // hdfsOpenFile
        hdfsFile writeFile = hdfsOpenFile(fs, rwPath,
                                          O_WRONLY|O_CREAT, 0, 0, 0);

        if(!writeFile) {
            fprintf(stderr, "Failed to open %s for writing!\n", rwPath);
            exit(1);
        }
        fprintf(stderr, "Opened %s for writing successfully...\n", rwPath);
        // hdfsWrite
        num_written_bytes = hdfsWrite(fs, writeFile, (void*)fileContents,
                                      (int) strlen(fileContents) + 1);
        if (num_written_bytes != strlen(fileContents) + 1) {
            fprintf(stderr, "Failed to write correct number of bytes - "
                    "expected %d, got %d\n",
                    (int)(strlen(fileContents) + 1), (int) num_written_bytes);
            exit(1);
        }
        fprintf(stderr, "Wrote %d bytes\n", num_written_bytes);
        
        // hdfsTell
        tOffset currentPos = -1;
        if ((currentPos = hdfsTell(fs, writeFile)) == -1) {
            fprintf(stderr,
                    "Failed to get current file position correctly. Got %"
                    PRId64 "!\n", currentPos);
            exit(1);
        }
        fprintf(stderr, "Current position: %" PRId64 "\n", currentPos);
        
        hdfsCloseFile(fs, writeFile);
        // Done test write
    }
    
    sleep(1);
    
    {
        //Read tests
        int available = 0, exists = 0;
        
        // hdfsExists
        exists = hdfsExists(fs, rwPath);
        if (exists) {
            fprintf(stderr, "Failed to validate existence of %s\n", rwPath);
            exists = hdfsExists(fs, rwPath);
            if (exists) {
                fprintf(stderr,
                        "Still failed to validate existence of %s\n", rwPath);
                exit(1);
            }
        }
        
        hdfsFile readFile = hdfsOpenFile(fs, rwPath, O_RDONLY, 0, 0, 0);
        if (!readFile) {
            fprintf(stderr, "Failed to open %s for reading!\n", rwPath);
            exit(1);
        }
        if (!hdfsFileIsOpenForRead(readFile)) {
            fprintf(stderr, "hdfsFileIsOpenForRead: we just opened a file "
                    "with O_RDONLY, and it did not show up as 'open for "
                    "read'\n");
            exit(1);
        }
        
        available = hdfsAvailable(fs, readFile);
        fprintf(stderr, "hdfsAvailable: %d\n", available);
        
        // hdfsSeek, hdfsTell
        tOffset seekPos = 1;
        if(hdfsSeek(fs, readFile, seekPos)) {
            fprintf(stderr, "Failed to seek %s for reading!\n", rwPath);
            exit(1);
        }
        
        tOffset currentPos = -1;
        if((currentPos = hdfsTell(fs, readFile)) != seekPos) {
            fprintf(stderr,
                    "Failed to get current file position correctly! Got %"
                    PRId64 "!\n", currentPos);

            exit(1);
        }
        fprintf(stderr, "Current position: %" PRId64 "\n", currentPos);
        
        if(hdfsSeek(fs, readFile, 0)) {
            fprintf(stderr, "Failed to seek %s for reading!\n", rwPath);
            exit(1);
        }
        
        // hdfsRead
        memset(buffer, 0, sizeof(buffer));
        tSize num_read_bytes = hdfsRead(fs, readFile, buffer, sizeof(buffer));
        if (strncmp(fileContents, buffer, strlen(fileContents)) != 0) {
            fprintf(stderr, "Failed to read (direct). "
                    "Expected %s but got %s (%d bytes)\n",
                    fileContents, buffer, num_read_bytes);
            exit(1);
        }
        fprintf(stderr, "Read following %d bytes:\n%s\n",
                num_read_bytes, buffer);
        
        if (hdfsSeek(fs, readFile, 0L)) {
            fprintf(stderr, "Failed to seek to file start!\n");
            exit(1);
        }
        
        // hdfsPread
        memset(buffer, 0, strlen(fileContents + 1));
        num_read_bytes = hdfsPread(fs, readFile, 0, buffer, sizeof(buffer));
        fprintf(stderr, "Read following %d bytes:\n%s\n",
                num_read_bytes, buffer);
        
        hdfsCloseFile(fs, readFile);
        // Done test read
    }
    
    int totalResult = 0;
    int result = 0;
    {
        //Generic file-system operations
        char *srcPath = rwPath;
        char buffer[256];
        const char *resp;
        rwTemplate2 = strdup("/tmp/helloWorld2XXXXXX");
        if (!rwTemplate2) {
            fprintf(stderr, "Failed to create rwTemplate2!\n");
            exit(1);
        }
        char *dstPath = mktemp(rwTemplate2);
        newDirTemplate = strdup("/tmp/newdirXXXXXX");
        if (!newDirTemplate) {
            fprintf(stderr, "Failed to create newDirTemplate!\n");
            exit(1);
        }
        char *newDirectory = mktemp(newDirTemplate);
        
        // hdfsRename
        fprintf(stderr, "hdfsRename: %s\n",
                ((result = hdfsRename(fs, rwPath, dstPath)) ?
                 "Failed!" : "Success!"));
        totalResult += result;
        fprintf(stderr, "hdfsRename back: %s\n",
                ((result = hdfsRename(fs, dstPath, srcPath)) ?
                 "Failed!" : "Success!"));
        totalResult += result;
        
        // hdfsCreateDirectory
        fprintf(stderr, "hdfsCreateDirectory: %s\n",
                ((result = hdfsCreateDirectory(fs, newDirectory)) ?
                 "Failed!" : "Success!"));
        totalResult += result;
        
        // hdfsSetReplication
        fprintf(stderr, "hdfsSetReplication: %s\n",
                ((result = hdfsSetReplication(fs, srcPath, 1)) ?
                 "Failed!" : "Success!"));
        totalResult += result;

        // hdfsGetWorkingDirectory, hdfsSetWorkingDirectory
        fprintf(stderr, "hdfsGetWorkingDirectory: %s\n",
                ((resp = hdfsGetWorkingDirectory(fs, buffer, sizeof(buffer))) ?
                 buffer : "Failed!"));
        totalResult += (resp ? 0 : 1);

        const char* path[] = {"/foo", "/foo/bar", "foobar", "//foo/bar//foobar",
                              "foo//bar", "foo/bar///", "/", "////"};
        int i;
        for (i = 0; i < 8; i++) {
            fprintf(stderr, "hdfsSetWorkingDirectory: %s, %s\n",
                    ((result = hdfsSetWorkingDirectory(fs, path[i])) ?
                     "Failed!" : "Success!"),
                    hdfsGetWorkingDirectory(fs, buffer, sizeof(buffer)));
            totalResult += result;
        }

        fprintf(stderr, "hdfsSetWorkingDirectory: %s\n",
                ((result = hdfsSetWorkingDirectory(fs, slashTmp)) ?
                 "Failed!" : "Success!"));
        totalResult += result;
        fprintf(stderr, "hdfsGetWorkingDirectory: %s\n",
                ((resp = hdfsGetWorkingDirectory(fs, buffer, sizeof(buffer))) ?
                 buffer : "Failed!"));
        totalResult += (resp ? 0 : 1);

        // hdfsGetPathInfo
        hdfsFileInfo *fileInfo = NULL;
        if((fileInfo = hdfsGetPathInfo(fs, slashTmp)) != NULL) {
            fprintf(stderr, "hdfsGetPathInfo - SUCCESS!\n");
            fprintf(stderr, "Name: %s, ", fileInfo->mName);
            fprintf(stderr, "Type: %c, ", (char)(fileInfo->mKind));
            fprintf(stderr, "Replication: %d, ", fileInfo->mReplication);
            fprintf(stderr, "BlockSize: %"PRId64", ", fileInfo->mBlockSize);
            fprintf(stderr, "Size: %"PRId64", ", fileInfo->mSize);
            fprintf(stderr, "LastMod: %s", ctime(&fileInfo->mLastMod));
            fprintf(stderr, "Owner: %s, ", fileInfo->mOwner);
            fprintf(stderr, "Group: %s, ", fileInfo->mGroup);
            char permissions[10];
            permission_disp(fileInfo->mPermissions, permissions);
            fprintf(stderr, "Permissions: %d (%s)\n",
                    fileInfo->mPermissions, permissions);
            hdfsFreeFileInfo(fileInfo, 1);
        } else {
            totalResult++;
            fprintf(stderr, "hdfsGetPathInfo for %s - FAILED!\n", slashTmp);
        }
        
        // hdfsListDirectory
        hdfsFileInfo *fileList = 0;
        int numEntries = 0;
        if((fileList = hdfsListDirectory(fs, slashTmp, &numEntries)) != NULL) {
            int i = 0;
            for(i=0; i < numEntries; ++i) {
                fprintf(stderr, "Name: %s, ", fileList[i].mName);
                fprintf(stderr, "Type: %c, ", (char)fileList[i].mKind);
                fprintf(stderr, "Replication: %d, ", fileList[i].mReplication);
                fprintf(stderr, "BlockSize: %"PRId64", ", fileList[i].mBlockSize);
                fprintf(stderr, "Size: %"PRId64", ", fileList[i].mSize);
                fprintf(stderr, "LastMod: %s", ctime(&fileList[i].mLastMod));
                fprintf(stderr, "Owner: %s, ", fileList[i].mOwner);
                fprintf(stderr, "Group: %s, ", fileList[i].mGroup);
                char permissions[10];
                permission_disp(fileList[i].mPermissions, permissions);
                fprintf(stderr, "Permissions: %d (%s)\n",
                        fileList[i].mPermissions, permissions);
            }
            hdfsFreeFileInfo(fileList, numEntries);
        } else {
            if (errno) {
                totalResult++;
                fprintf(stderr, "waah! hdfsListDirectory - FAILED!\n");
            } else {
                fprintf(stderr, "Empty directory!\n");
            }
        }
        
        char *newOwner = "root";
        // Setting tmp dir to 777 so later when connectAsUser nobody,
        // we can write to it
        short newPerm = 0666;
        
        // hdfsChown
        fprintf(stderr, "hdfsChown: %s\n",
                ((result = hdfsChown(fs, rwPath, NULL, "users")) ?
                 "Failed!" : "Success!"));
        totalResult += result;
        fprintf(stderr, "hdfsChown: %s\n",
                ((result = hdfsChown(fs, rwPath, newOwner, NULL)) ?
                 "Failed!" : "Success!"));
        totalResult += result;
        // hdfsChmod
        fprintf(stderr, "hdfsChmod: %s\n",
                ((result = hdfsChmod(fs, rwPath, newPerm)) ?
                 "Failed!" : "Success!"));
        totalResult += result;
        
        sleep(2);
        tTime newMtime = time(NULL);
        tTime newAtime = time(NULL);
        
        // utime write
        fprintf(stderr, "hdfsUtime: %s\n",
                ((result = hdfsUtime(fs, rwPath, newMtime, newAtime)) ?
                 "Failed!" : "Success!"));        
        totalResult += result;
        
        // chown/chmod/utime read
        hdfsFileInfo *finfo = hdfsGetPathInfo(fs, rwPath);
        
        fprintf(stderr, "hdfsChown read: %s\n",
                ((result = (strcmp(finfo->mOwner, newOwner) != 0)) ?
                 "Failed!" : "Success!"));
        totalResult += result;
        
        fprintf(stderr, "hdfsChmod read: %s\n",
                ((result = (finfo->mPermissions != newPerm)) ?
                 "Failed!" : "Success!"));
        totalResult += result;
        
        // will later use /tmp/ as a different user so enable it
        fprintf(stderr, "hdfsChmod: %s\n",
                ((result = hdfsChmod(fs, slashTmp, 0777)) ?
                 "Failed!" : "Success!"));
        totalResult += result;
        
        fprintf(stderr,"newMTime=%ld\n",newMtime);
        fprintf(stderr,"curMTime=%ld\n",finfo->mLastMod);
        
        
        fprintf(stderr, "hdfsUtime read (mtime): %s\n",
                ((result = (finfo->mLastMod != newMtime / 1000)) ?
                 "Failed!" : "Success!"));
        totalResult += result;
        
        // Clean up
        hdfsFreeFileInfo(finfo, 1);
        fprintf(stderr, "hdfsDelete: %s\n",
                ((result = hdfsDelete(fs, newDirectory, 1)) ?
                 "Failed!" : "Success!"));
        totalResult += result;
        fprintf(stderr, "hdfsDelete: %s\n",
                ((result = hdfsDelete(fs, srcPath, 1)) ?
                 "Failed!" : "Success!"));
        totalResult += result;
        fprintf(stderr, "hdfsExists: %s\n",
                ((result = hdfsExists(fs, newDirectory)) ?
                 "Success!" : "Failed!"));
        totalResult += (result ? 0 : 1);
        // Done test generic operations
    }
    
    {
        // Test Appends
        appendTemplate = strdup("/tmp/appendsXXXXXX");
        if (!appendTemplate) {
            fprintf(stderr, "Failed to create appendTemplate!\n");
            exit(1);
        }
        char *appendPath = mktemp(appendTemplate);
        const char* helloBuffer = "Hello,";
        hdfsFile writeFile = NULL;
        
        // Create
        writeFile = hdfsOpenFile(fs, appendPath, O_WRONLY, 0, 0, 0);
        if(!writeFile) {
            fprintf(stderr, "Failed to open %s for writing!\n", appendPath);
            exit(1);
        }
        fprintf(stderr, "Opened %s for writing successfully...\n", appendPath);
        
        num_written_bytes = hdfsWrite(fs, writeFile, helloBuffer,
                                      (int) strlen(helloBuffer));
        fprintf(stderr, "Wrote %d bytes\n", num_written_bytes);
        hdfsCloseFile(fs, writeFile);
        
        fprintf(stderr, "hdfsSetReplication: %s\n",
                ((result = hdfsSetReplication(fs, appendPath, 1)) ?
                 "Failed!" : "Success!"));
        totalResult += result;
        
        // Re-Open for Append
        writeFile = hdfsOpenFile(fs, appendPath, O_WRONLY | O_APPEND, 0, 0, 0);
        if(!writeFile) {
            fprintf(stderr, "Failed to open %s for writing!\n", appendPath);
            exit(1);
        }
        fprintf(stderr, "Opened %s for appending successfully...\n",
                appendPath);
        
        helloBuffer = " World";
        num_written_bytes = hdfsWrite(fs, writeFile, helloBuffer,
                                      (int)strlen(helloBuffer) + 1);
        fprintf(stderr, "Wrote %d bytes\n", num_written_bytes);
        
        hdfsCloseFile(fs, writeFile);

        // Check size
        hdfsFileInfo *finfo = hdfsGetPathInfo(fs, appendPath);
        fprintf(stderr, "fileinfo->mSize: == total %s\n",
                ((result = (finfo->mSize == strlen("Hello, World") + 1)) ?
                 "Success!" : "Failed!"));
        totalResult += (result ? 0 : 1);
        
        // Read and check data
        hdfsFile readFile = hdfsOpenFile(fs, appendPath, O_RDONLY, 0, 0, 0);
        if (!readFile) {
            fprintf(stderr, "Failed to open %s for reading!\n", appendPath);
            exit(1);
        }
        
        tSize num_read_bytes = hdfsRead(fs, readFile, buffer, sizeof(buffer));
        fprintf(stderr, "Read following %d bytes:\n%s\n",
                num_read_bytes, buffer);
        fprintf(stderr, "read == Hello, World %s\n",
                (result = (strcmp(buffer, "Hello, World") == 0)) ?
                "Success!" : "Failed!");
        hdfsCloseFile(fs, readFile);
        
        // Cleanup
        fprintf(stderr, "hdfsDelete: %s\n",
                ((result = hdfsDelete(fs, appendPath, 1)) ?
                 "Failed!" : "Success!"));
        totalResult += result;
        // Done test appends
    }
    
    totalResult += (hdfsDisconnect(fs) != 0);
    
    {
        //
        // Now test as connecting as a specific user
        // This only meant to test that we connected as that user, not to test
        // the actual fs user capabilities. Thus just create a file and read
        // the owner is correct.
        const char *tuser = "nobody";
        userTemplate = strdup("/tmp/usertestXXXXXX");
        if (!userTemplate) {
            fprintf(stderr, "Failed to create userTemplate!\n");
            exit(1);
        }
        char* userWritePath = mktemp(userTemplate);
        hdfsFile writeFile = NULL;
        
        fs = hdfsConnectAsUserNewInstance("default", 50070, tuser);
        if(!fs) {
            fprintf(stderr,
                    "Oops! Failed to connect to hdfs as user %s!\n",tuser);
            exit(1);
        }
        
        writeFile = hdfsOpenFile(fs, userWritePath, O_WRONLY|O_CREAT, 0, 0, 0);
        if(!writeFile) {
            fprintf(stderr, "Failed to open %s for writing!\n", userWritePath);
            exit(1);
        }
        fprintf(stderr, "Opened %s for writing successfully...\n",
                userWritePath);
        
        num_written_bytes = hdfsWrite(fs, writeFile, fileContents,
                                      (int)strlen(fileContents) + 1);
        fprintf(stderr, "Wrote %d bytes\n", num_written_bytes);
        hdfsCloseFile(fs, writeFile);
        
        hdfsFileInfo *finfo = hdfsGetPathInfo(fs, userWritePath);
        if (finfo) {
            fprintf(stderr, "hdfs new file user is correct: %s\n",
                    ((result = (strcmp(finfo->mOwner, tuser) != 0)) ?
                     "Failed!" : "Success!"));
        } else {
            fprintf(stderr,
                    "hdfsFileInfo returned by hdfsGetPathInfo is NULL\n");
            result = -1;
        }
        totalResult += result;
        
        // Cleanup
        fprintf(stderr, "hdfsDelete: %s\n",
                ((result = hdfsDelete(fs, userWritePath, 1)) ?
                 "Failed!" : "Success!"));
        totalResult += result;
        // Done test specific user
    }

    totalResult += (hdfsDisconnect(fs) != 0);
    
    // Shutdown the native minidfscluster
    nmdShutdown(cluster);
    nmdFree(cluster);
    
    fprintf(stderr, "totalResult == %d\n", totalResult);
    if (totalResult != 0) {
        return -1;
    } else {
        return 0;
    }
}

/**
 * vim: ts=4: sw=4: et:
 */
