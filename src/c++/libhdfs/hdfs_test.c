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

int main(int argc, char **argv) {

    hdfsFS fs = hdfsConnect("default", 0);
    if(!fs) {
        fprintf(stderr, "Oops! Failed to connect to hdfs!\n");
        exit(-1);
    } 
 
    hdfsFS lfs = hdfsConnect(NULL, 0);
    if(!fs) {
        fprintf(stderr, "Oops! Failed to connect to 'local' hdfs!\n");
        exit(-1);
    } 
 
    {
        //Write tests
        
        const char* writePath = "/tmp/testfile.txt";
        
        hdfsFile writeFile = hdfsOpenFile(fs, writePath, O_WRONLY, 0, 0, 0);
        if(!writeFile) {
            fprintf(stderr, "Failed to open %s for writing!\n", writePath);
            exit(-1);
        }
        fprintf(stderr, "Opened %s for writing successfully...\n", writePath);

        char* buffer = "Hello, World!";
        tSize num_written_bytes = hdfsWrite(fs, writeFile, (void*)buffer, strlen(buffer)+1);
        fprintf(stderr, "Wrote %d bytes\n", num_written_bytes);

        tOffset currentPos = -1;
        if ((currentPos = hdfsTell(fs, writeFile)) == -1) {
            fprintf(stderr, 
                    "Failed to get current file position correctly! Got %d!\n",
                    currentPos);
            exit(-1);
        }
        fprintf(stderr, "Current position: %ld\n", currentPos);

        if (hdfsFlush(fs, writeFile)) {
            fprintf(stderr, "Failed to 'flush' %s\n", writePath); 
            exit(-1);
        }
        fprintf(stderr, "Flushed %s successfully!\n", writePath); 

        hdfsCloseFile(fs, writeFile);
    }

    {
        //Read tests
        
        const char* readPath = "/tmp/testfile.txt";
        int exists = hdfsExists(fs, readPath);

        if (exists) {
          fprintf(stderr, "Failed to validate existence of %s\n", readPath);
          exit(-1);
        }

        hdfsFile readFile = hdfsOpenFile(fs, readPath, O_RDONLY, 0, 0, 0);
        if (!readFile) {
            fprintf(stderr, "Failed to open %s for reading!\n", readPath);
            exit(-1);
        }

        fprintf(stderr, "hdfsAvailable: %d\n", hdfsAvailable(fs, readFile));

        tOffset seekPos = 1;
        if(hdfsSeek(fs, readFile, seekPos)) {
            fprintf(stderr, "Failed to seek %s for reading!\n", readPath);
            exit(-1);
        }

        tOffset currentPos = -1;
        if((currentPos = hdfsTell(fs, readFile)) != seekPos) {
            fprintf(stderr, 
                    "Failed to get current file position correctly! Got %d!\n", 
                    currentPos);
            exit(-1);
        }
        fprintf(stderr, "Current position: %ld\n", currentPos);

        static char buffer[32];
        tSize num_read_bytes = hdfsRead(fs, readFile, (void*)buffer, 
                sizeof(buffer));
        fprintf(stderr, "Read following %d bytes:\n%s\n", 
                num_read_bytes, buffer);

        num_read_bytes = hdfsPread(fs, readFile, 0, (void*)buffer, 
                sizeof(buffer));
        fprintf(stderr, "Read following %d bytes:\n%s\n", 
                num_read_bytes, buffer);

        hdfsCloseFile(fs, readFile);
    }

 
    {
        //Generic file-system operations

        const char* srcPath = "/tmp/testfile.txt";
        const char* dstPath = "/tmp/testfile2.txt";

        fprintf(stderr, "hdfsCopy(remote-local): %s\n", (hdfsCopy(fs, srcPath, lfs, srcPath) ? "Failed!" : "Success!"));
        fprintf(stderr, "hdfsCopy(remote-remote): %s\n", (hdfsCopy(fs, srcPath, fs, dstPath) ? "Failed!" : "Success!"));
        fprintf(stderr, "hdfsMove(local-local): %s\n", (hdfsMove(lfs, srcPath, lfs, dstPath) ? "Failed!" : "Success!"));
        fprintf(stderr, "hdfsMove(remote-local): %s\n", (hdfsMove(fs, srcPath, lfs, srcPath) ? "Failed!" : "Success!"));

        fprintf(stderr, "hdfsRename: %s\n", (hdfsRename(fs, dstPath, srcPath) ? "Failed!" : "Success!"));

        fprintf(stderr, "hdfsLock: %s\n", (hdfsLock(fs, srcPath, 1) ? "Failed!" : "Success!"));
        fprintf(stderr, "hdfsReleaseLock: %s\n", (hdfsReleaseLock(fs, srcPath) ? "Failed!" : "Success!"));

        const char* slashTmp = "/tmp";
        const char* newDirectory = "/tmp/newdir";
        fprintf(stderr, "hdfsCreateDirectory: %s\n", (hdfsCreateDirectory(fs, newDirectory) ? "Failed!" : "Success!"));

        char buffer[256];
        fprintf(stderr, "hdfsGetWorkingDirectory: %s\n", (hdfsGetWorkingDirectory(fs, buffer, sizeof(buffer)) ? buffer : "Failed!"));
        fprintf(stderr, "hdfsSetWorkingDirectory: %s\n", (hdfsSetWorkingDirectory(fs, slashTmp) ? "Failed!" : "Success!"));
        fprintf(stderr, "hdfsGetWorkingDirectory: %s\n", (hdfsGetWorkingDirectory(fs, buffer, sizeof(buffer)) ? buffer : "Failed!"));

        fprintf(stderr, "hdfsGetDefaultBlockSize: %Ld\n", hdfsGetDefaultBlockSize(fs));
        fprintf(stderr, "hdfsGetCapacity: %Ld\n", hdfsGetCapacity(fs));
        fprintf(stderr, "hdfsGetUsed: %Ld\n", hdfsGetUsed(fs));

        hdfsFileInfo *fileInfo = NULL;
        if(fileInfo = hdfsGetPathInfo(fs, slashTmp)) {
            fprintf(stderr, "hdfsGetPathInfo - SUCCESS!\n");
            fprintf(stderr, "Name: %s,", fileInfo->mName);
            fprintf(stderr, "Type: %c,", (char)fileInfo->mKind);
            fprintf(stderr, "Size: %ld\n", fileInfo->mSize);
            hdfsFreeFileInfo(fileInfo, 1);
        } else {
            fprintf(stderr, "waah! hdfsGetPathInfo for %s - FAILED!\n", slashTmp);
        }

        hdfsFileInfo *fileList = 0;
        int numEntries = 0;
        if(fileList = hdfsListDirectory(fs, slashTmp, &numEntries)) {
            int i = 0;
            for(i=0; i < numEntries; ++i) {
                fprintf(stderr, "Name: %s,", fileList[i].mName);
                fprintf(stderr, "Type: %c,", (char)fileList[i].mKind);
                fprintf(stderr, "Size: %ld\n", fileList[i].mSize);
            }
            hdfsFreeFileInfo(fileList, numEntries);
        } else {
            if (errno) {
                fprintf(stderr, "waah! hdfsListDirectory - FAILED!\n");
            } else {
                fprintf(stderr, "Empty directory!\n");
            }
        }

        char*** hosts = hdfsGetHosts(fs, srcPath, 0, 1);
        if(hosts) {
            fprintf(stderr, "hdfsGetHosts - SUCCESS! ... \n");
            int i=0; 
            while(hosts[i]) {
                int j = 0;
                while(hosts[i][j]) {
                    fprintf(stderr, 
                            "\thosts[%d][%d] - %s\n", i, j, hosts[i][j]);
                    ++j;
                }
                ++i;
            }
        } else {
            fprintf(stderr, "waah! hdfsGetHosts - FAILED!\n");
        }
        
        // Clean up
        fprintf(stderr, "hdfsDelete: %s\n", (hdfsDelete(fs, newDirectory) ? "Failed!" : "Success!"));
        fprintf(stderr, "hdfsDelete: %s\n", (hdfsDelete(fs, srcPath) ? "Failed!" : "Success!"));
        fprintf(stderr, "hdfsDelete: %s\n", (hdfsDelete(lfs, srcPath) ? "Failed!" : "Success!"));
        fprintf(stderr, "hdfsDelete: %s\n", (hdfsDelete(lfs, dstPath) ? "Failed!" : "Success!"));
        fprintf(stderr, "hdfsExists: %s\n", (hdfsExists(fs, newDirectory) ? "Success!" : "Failed!"));
    }

    return 0;
}

/**
 * vim: ts=4: sw=4: et:
 */

