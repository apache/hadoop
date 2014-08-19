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
#include "hdfs_test.h" 
#include "platform.h"

#include <inttypes.h>
#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

void permission_disp(short permissions, char *rtr) {
  int i;
  short permissionsId;
  char* perm;
  rtr[9] = '\0';
  for(i=2;i>=0;i--)
    {
      permissionsId = permissions >> (i * 3) & (short)7;
      switch(permissionsId) {
      case 7:
        perm = "rwx"; break;
      case 6:
        perm = "rw-"; break;
      case 5:
        perm = "r-x"; break;
      case 4:
        perm = "r--"; break;
      case 3:
        perm = "-wx"; break;
      case 2:
        perm = "-w-"; break;
      case 1:
        perm = "--x"; break;
      case 0:
        perm = "---"; break;
      default:
        perm = "???";
      }
      strncpy(rtr, perm, 3);
      rtr+=3;
    }
} 

int main(int argc, char **argv) {
    const char *writePath = "/tmp/testfile.txt";
    const char *fileContents = "Hello, World!";
    const char *readPath = "/tmp/testfile.txt";
    const char *srcPath = "/tmp/testfile.txt";
    const char *dstPath = "/tmp/testfile2.txt";
    const char *slashTmp = "/tmp";
    const char *newDirectory = "/tmp/newdir";
    const char *newOwner = "root";
    const char *tuser = "nobody";
    const char *appendPath = "/tmp/appends";
    const char *userPath = "/tmp/usertestfile.txt";

    char buffer[32], buffer2[256], rdbuffer[32];
    tSize num_written_bytes, num_read_bytes;
    hdfsFS fs, lfs;
    hdfsFile writeFile, readFile, localFile, appendFile, userFile;
    tOffset currentPos, seekPos;
    int exists, totalResult, result, numEntries, i, j;
    const char *resp;
    hdfsFileInfo *fileInfo, *fileList, *finfo;
    char *buffer3;
    char permissions[10];
    char ***hosts;
    short newPerm = 0666;
    tTime newMtime, newAtime;

    fs = hdfsConnectNewInstance("default", 0);
    if(!fs) {
        fprintf(stderr, "Oops! Failed to connect to hdfs!\n");
        exit(-1);
    } 
 
    lfs = hdfsConnectNewInstance(NULL, 0);
    if(!lfs) {
        fprintf(stderr, "Oops! Failed to connect to 'local' hdfs!\n");
        exit(-1);
    } 

    {
        //Write tests
        
        writeFile = hdfsOpenFile(fs, writePath, O_WRONLY|O_CREAT, 0, 0, 0);
        if(!writeFile) {
            fprintf(stderr, "Failed to open %s for writing!\n", writePath);
            exit(-1);
        }
        fprintf(stderr, "Opened %s for writing successfully...\n", writePath);
        num_written_bytes =
          hdfsWrite(fs, writeFile, (void*)fileContents,
            (tSize)(strlen(fileContents)+1));
        if (num_written_bytes != strlen(fileContents) + 1) {
          fprintf(stderr, "Failed to write correct number of bytes - expected %d, got %d\n",
                  (int)(strlen(fileContents) + 1), (int)num_written_bytes);
            exit(-1);
        }
        fprintf(stderr, "Wrote %d bytes\n", num_written_bytes);

        currentPos = -1;
        if ((currentPos = hdfsTell(fs, writeFile)) == -1) {
            fprintf(stderr, 
                    "Failed to get current file position correctly! Got %ld!\n",
                    currentPos);
            exit(-1);
        }
        fprintf(stderr, "Current position: %ld\n", currentPos);

        if (hdfsFlush(fs, writeFile)) {
            fprintf(stderr, "Failed to 'flush' %s\n", writePath); 
            exit(-1);
        }
        fprintf(stderr, "Flushed %s successfully!\n", writePath); 

        if (hdfsHFlush(fs, writeFile)) {
            fprintf(stderr, "Failed to 'hflush' %s\n", writePath);
            exit(-1);
        }
        fprintf(stderr, "HFlushed %s successfully!\n", writePath);

        hdfsCloseFile(fs, writeFile);
    }

    {
        //Read tests
        
        exists = hdfsExists(fs, readPath);

        if (exists) {
          fprintf(stderr, "Failed to validate existence of %s\n", readPath);
          exit(-1);
        }

        readFile = hdfsOpenFile(fs, readPath, O_RDONLY, 0, 0, 0);
        if (!readFile) {
            fprintf(stderr, "Failed to open %s for reading!\n", readPath);
            exit(-1);
        }

        if (!hdfsFileIsOpenForRead(readFile)) {
            fprintf(stderr, "hdfsFileIsOpenForRead: we just opened a file "
                    "with O_RDONLY, and it did not show up as 'open for "
                    "read'\n");
            exit(-1);
        }

        fprintf(stderr, "hdfsAvailable: %d\n", hdfsAvailable(fs, readFile));

        seekPos = 1;
        if(hdfsSeek(fs, readFile, seekPos)) {
            fprintf(stderr, "Failed to seek %s for reading!\n", readPath);
            exit(-1);
        }

        currentPos = -1;
        if((currentPos = hdfsTell(fs, readFile)) != seekPos) {
            fprintf(stderr, 
                    "Failed to get current file position correctly! Got %ld!\n", 
                    currentPos);
            exit(-1);
        }
        fprintf(stderr, "Current position: %ld\n", currentPos);

        if (!hdfsFileUsesDirectRead(readFile)) {
          fprintf(stderr, "Direct read support incorrectly not detected "
                  "for HDFS filesystem\n");
          exit(-1);
        }

        fprintf(stderr, "Direct read support detected for HDFS\n");

        // Test the direct read path
        if(hdfsSeek(fs, readFile, 0)) {
            fprintf(stderr, "Failed to seek %s for reading!\n", readPath);
            exit(-1);
        }
        memset(buffer, 0, sizeof(buffer));
        num_read_bytes = hdfsRead(fs, readFile, (void*)buffer,
                sizeof(buffer));
        if (strncmp(fileContents, buffer, strlen(fileContents)) != 0) {
            fprintf(stderr, "Failed to read (direct). Expected %s but got %s (%d bytes)\n",
                    fileContents, buffer, num_read_bytes);
            exit(-1);
        }
        fprintf(stderr, "Read (direct) following %d bytes:\n%s\n",
                num_read_bytes, buffer);
        if (hdfsSeek(fs, readFile, 0L)) {
            fprintf(stderr, "Failed to seek to file start!\n");
            exit(-1);
        }

        // Disable the direct read path so that we really go through the slow
        // read path
        hdfsFileDisableDirectRead(readFile);

        num_read_bytes = hdfsRead(fs, readFile, (void*)buffer, 
                sizeof(buffer));
        fprintf(stderr, "Read following %d bytes:\n%s\n", 
                num_read_bytes, buffer);

        memset(buffer, 0, strlen(fileContents + 1));

        num_read_bytes = hdfsPread(fs, readFile, 0, (void*)buffer, 
                sizeof(buffer));
        fprintf(stderr, "Read following %d bytes:\n%s\n", 
                num_read_bytes, buffer);

        hdfsCloseFile(fs, readFile);

        // Test correct behaviour for unsupported filesystems
        localFile = hdfsOpenFile(lfs, writePath, O_WRONLY|O_CREAT, 0, 0, 0);
        if(!localFile) {
            fprintf(stderr, "Failed to open %s for writing!\n", writePath);
            exit(-1);
        }

        num_written_bytes = hdfsWrite(lfs, localFile, (void*)fileContents,
                                      (tSize)(strlen(fileContents) + 1));

        hdfsCloseFile(lfs, localFile);
        localFile = hdfsOpenFile(lfs, writePath, O_RDONLY, 0, 0, 0);

        if (hdfsFileUsesDirectRead(localFile)) {
          fprintf(stderr, "Direct read support incorrectly detected for local "
                  "filesystem\n");
          exit(-1);
        }

        hdfsCloseFile(lfs, localFile);
    }

    totalResult = 0;
    result = 0;
    {
        //Generic file-system operations

        fprintf(stderr, "hdfsCopy(remote-local): %s\n", ((result = hdfsCopy(fs, srcPath, lfs, srcPath)) != 0 ? "Failed!" : "Success!"));
        totalResult += result;
        fprintf(stderr, "hdfsCopy(remote-remote): %s\n", ((result = hdfsCopy(fs, srcPath, fs, dstPath)) != 0 ? "Failed!" : "Success!"));
        totalResult += result;
        fprintf(stderr, "hdfsMove(local-local): %s\n", ((result = hdfsMove(lfs, srcPath, lfs, dstPath)) != 0 ? "Failed!" : "Success!"));
        totalResult += result;
        fprintf(stderr, "hdfsMove(remote-local): %s\n", ((result = hdfsMove(fs, srcPath, lfs, srcPath)) != 0 ? "Failed!" : "Success!"));
        totalResult += result;

        fprintf(stderr, "hdfsRename: %s\n", ((result = hdfsRename(fs, dstPath, srcPath)) != 0 ? "Failed!" : "Success!"));
        totalResult += result;
        fprintf(stderr, "hdfsCopy(remote-remote): %s\n", ((result = hdfsCopy(fs, srcPath, fs, dstPath)) != 0 ? "Failed!" : "Success!"));
        totalResult += result;

        fprintf(stderr, "hdfsCreateDirectory: %s\n", ((result = hdfsCreateDirectory(fs, newDirectory)) != 0 ? "Failed!" : "Success!"));
        totalResult += result;

        fprintf(stderr, "hdfsSetReplication: %s\n", ((result = hdfsSetReplication(fs, srcPath, 2)) != 0 ? "Failed!" : "Success!"));
        totalResult += result;

        fprintf(stderr, "hdfsGetWorkingDirectory: %s\n", ((resp = hdfsGetWorkingDirectory(fs, buffer2, sizeof(buffer2))) != 0 ? buffer2 : "Failed!"));
        totalResult += (resp ? 0 : 1);
        fprintf(stderr, "hdfsSetWorkingDirectory: %s\n", ((result = hdfsSetWorkingDirectory(fs, slashTmp)) != 0 ? "Failed!" : "Success!"));
        totalResult += result;
        fprintf(stderr, "hdfsGetWorkingDirectory: %s\n", ((resp = hdfsGetWorkingDirectory(fs, buffer2, sizeof(buffer2))) != 0 ? buffer2 : "Failed!"));
        totalResult += (resp ? 0 : 1);

        fprintf(stderr, "hdfsGetDefaultBlockSize: %ld\n", hdfsGetDefaultBlockSize(fs));
        fprintf(stderr, "hdfsGetCapacity: %ld\n", hdfsGetCapacity(fs));
        fprintf(stderr, "hdfsGetUsed: %ld\n", hdfsGetUsed(fs));

        fileInfo = NULL;
        if((fileInfo = hdfsGetPathInfo(fs, slashTmp)) != NULL) {
            fprintf(stderr, "hdfsGetPathInfo - SUCCESS!\n");
            fprintf(stderr, "Name: %s, ", fileInfo->mName);
            fprintf(stderr, "Type: %c, ", (char)(fileInfo->mKind));
            fprintf(stderr, "Replication: %d, ", fileInfo->mReplication);
            fprintf(stderr, "BlockSize: %ld, ", fileInfo->mBlockSize);
            fprintf(stderr, "Size: %ld, ", fileInfo->mSize);
            fprintf(stderr, "LastMod: %s", ctime(&fileInfo->mLastMod)); 
            fprintf(stderr, "Owner: %s, ", fileInfo->mOwner);
            fprintf(stderr, "Group: %s, ", fileInfo->mGroup);
            permission_disp(fileInfo->mPermissions, permissions);
            fprintf(stderr, "Permissions: %d (%s)\n", fileInfo->mPermissions, permissions);
            hdfsFreeFileInfo(fileInfo, 1);
        } else {
            totalResult++;
            fprintf(stderr, "waah! hdfsGetPathInfo for %s - FAILED!\n", slashTmp);
        }

        fileList = 0;
        if((fileList = hdfsListDirectory(fs, slashTmp, &numEntries)) != NULL) {
            for(i=0; i < numEntries; ++i) {
                fprintf(stderr, "Name: %s, ", fileList[i].mName);
                fprintf(stderr, "Type: %c, ", (char)fileList[i].mKind);
                fprintf(stderr, "Replication: %d, ", fileList[i].mReplication);
                fprintf(stderr, "BlockSize: %ld, ", fileList[i].mBlockSize);
                fprintf(stderr, "Size: %ld, ", fileList[i].mSize);
                fprintf(stderr, "LastMod: %s", ctime(&fileList[i].mLastMod));
                fprintf(stderr, "Owner: %s, ", fileList[i].mOwner);
                fprintf(stderr, "Group: %s, ", fileList[i].mGroup);
                permission_disp(fileList[i].mPermissions, permissions);
                fprintf(stderr, "Permissions: %d (%s)\n", fileList[i].mPermissions, permissions);
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

        hosts = hdfsGetHosts(fs, srcPath, 0, 1);
        if(hosts) {
            fprintf(stderr, "hdfsGetHosts - SUCCESS! ... \n");
            i=0; 
            while(hosts[i]) {
                j = 0;
                while(hosts[i][j]) {
                    fprintf(stderr, 
                            "\thosts[%d][%d] - %s\n", i, j, hosts[i][j]);
                    ++j;
                }
                ++i;
            }
        } else {
            totalResult++;
            fprintf(stderr, "waah! hdfsGetHosts - FAILED!\n");
        }
       
        // setting tmp dir to 777 so later when connectAsUser nobody, we can write to it

        // chown write
        fprintf(stderr, "hdfsChown: %s\n", ((result = hdfsChown(fs, writePath, NULL, "users")) != 0 ? "Failed!" : "Success!"));
        totalResult += result;
        fprintf(stderr, "hdfsChown: %s\n", ((result = hdfsChown(fs, writePath, newOwner, NULL)) != 0 ? "Failed!" : "Success!"));
        totalResult += result;
        // chmod write
        fprintf(stderr, "hdfsChmod: %s\n", ((result = hdfsChmod(fs, writePath, newPerm)) != 0 ? "Failed!" : "Success!"));
        totalResult += result;



        sleep(2);
        newMtime = time(NULL);
        newAtime = time(NULL);

        // utime write
        fprintf(stderr, "hdfsUtime: %s\n", ((result = hdfsUtime(fs, writePath, newMtime, newAtime)) != 0 ? "Failed!" : "Success!"));

        totalResult += result;

        // chown/chmod/utime read
        finfo = hdfsGetPathInfo(fs, writePath);

        fprintf(stderr, "hdfsChown read: %s\n", ((result = (strcmp(finfo->mOwner, newOwner))) != 0 ? "Failed!" : "Success!"));
        totalResult += result;

        fprintf(stderr, "hdfsChmod read: %s\n", ((result = (finfo->mPermissions != newPerm)) != 0 ? "Failed!" : "Success!"));
        totalResult += result;

        // will later use /tmp/ as a different user so enable it
        fprintf(stderr, "hdfsChmod: %s\n", ((result = hdfsChmod(fs, "/tmp/", 0777)) != 0 ? "Failed!" : "Success!"));
        totalResult += result;

        fprintf(stderr,"newMTime=%ld\n",newMtime);
        fprintf(stderr,"curMTime=%ld\n",finfo->mLastMod);


        fprintf(stderr, "hdfsUtime read (mtime): %s\n", ((result = (finfo->mLastMod != newMtime)) != 0 ? "Failed!" : "Success!"));
        totalResult += result;

        // No easy way to turn on access times from hdfs_test right now
        //        fprintf(stderr, "hdfsUtime read (atime): %s\n", ((result = (finfo->mLastAccess != newAtime)) != 0 ? "Failed!" : "Success!"));
        //        totalResult += result;

        hdfsFreeFileInfo(finfo, 1);

        // Clean up
        fprintf(stderr, "hdfsDelete: %s\n", ((result = hdfsDelete(fs, newDirectory, 1)) != 0 ? "Failed!" : "Success!"));
        totalResult += result;
        fprintf(stderr, "hdfsDelete: %s\n", ((result = hdfsDelete(fs, srcPath, 1)) != 0 ? "Failed!" : "Success!"));
        totalResult += result;
        fprintf(stderr, "hdfsDelete: %s\n", ((result = hdfsDelete(lfs, srcPath, 1)) != 0 ? "Failed!" : "Success!"));
        totalResult += result;
        fprintf(stderr, "hdfsDelete: %s\n", ((result = hdfsDelete(lfs, dstPath, 1)) != 0 ? "Failed!" : "Success!"));
        totalResult += result;
        fprintf(stderr, "hdfsExists: %s\n", ((result = hdfsExists(fs, newDirectory)) != 0 ? "Success!" : "Failed!"));
        totalResult += (result ? 0 : 1);
    }

    {
      // TEST APPENDS

      // CREATE
      appendFile = hdfsOpenFile(fs, appendPath, O_WRONLY, 0, 0, 0);
      if(!appendFile) {
        fprintf(stderr, "Failed to open %s for writing!\n", appendPath);
        exit(-1);
      }
      fprintf(stderr, "Opened %s for writing successfully...\n", appendPath);

      buffer3 = "Hello,";
      num_written_bytes = hdfsWrite(fs, appendFile, (void*)buffer3,
        (tSize)strlen(buffer3));
      fprintf(stderr, "Wrote %d bytes\n", num_written_bytes);

      if (hdfsFlush(fs, appendFile)) {
        fprintf(stderr, "Failed to 'flush' %s\n", appendPath); 
        exit(-1);
        }
      fprintf(stderr, "Flushed %s successfully!\n", appendPath); 

      hdfsCloseFile(fs, appendFile);

      // RE-OPEN
      appendFile = hdfsOpenFile(fs, appendPath, O_WRONLY|O_APPEND, 0, 0, 0);
      if(!appendFile) {
        fprintf(stderr, "Failed to open %s for writing!\n", appendPath);
        exit(-1);
      }
      fprintf(stderr, "Opened %s for writing successfully...\n", appendPath);

      buffer3 = " World";
      num_written_bytes = hdfsWrite(fs, appendFile, (void*)buffer3,
        (tSize)(strlen(buffer3) + 1));
      fprintf(stderr, "Wrote %d bytes\n", num_written_bytes);

      if (hdfsFlush(fs, appendFile)) {
        fprintf(stderr, "Failed to 'flush' %s\n", appendPath); 
        exit(-1);
      }
      fprintf(stderr, "Flushed %s successfully!\n", appendPath); 

      hdfsCloseFile(fs, appendFile);

      // CHECK size
      finfo = hdfsGetPathInfo(fs, appendPath);
      fprintf(stderr, "fileinfo->mSize: == total %s\n", ((result = (finfo->mSize == (tOffset)(strlen("Hello, World") + 1))) == 1 ? "Success!" : "Failed!"));
      totalResult += (result ? 0 : 1);

      // READ and check data
      readFile = hdfsOpenFile(fs, appendPath, O_RDONLY, 0, 0, 0);
      if (!readFile) {
        fprintf(stderr, "Failed to open %s for reading!\n", appendPath);
        exit(-1);
      }

      num_read_bytes = hdfsRead(fs, readFile, (void*)rdbuffer, sizeof(rdbuffer));
      fprintf(stderr, "Read following %d bytes:\n%s\n", 
              num_read_bytes, rdbuffer);

      fprintf(stderr, "read == Hello, World %s\n", ((result = (strcmp(rdbuffer, "Hello, World"))) == 0 ? "Success!" : "Failed!"));

      hdfsCloseFile(fs, readFile);

      // DONE test appends
    }
      
      
    totalResult += (hdfsDisconnect(fs) != 0);

    {
      //
      // Now test as connecting as a specific user
      // This is only meant to test that we connected as that user, not to test
      // the actual fs user capabilities. Thus just create a file and read
      // the owner is correct.

      fs = hdfsConnectAsUserNewInstance("default", 0, tuser);
      if(!fs) {
        fprintf(stderr, "Oops! Failed to connect to hdfs as user %s!\n",tuser);
        exit(-1);
      } 

        userFile = hdfsOpenFile(fs, userPath, O_WRONLY|O_CREAT, 0, 0, 0);
        if(!userFile) {
            fprintf(stderr, "Failed to open %s for writing!\n", userPath);
            exit(-1);
        }
        fprintf(stderr, "Opened %s for writing successfully...\n", userPath);

        num_written_bytes = hdfsWrite(fs, userFile, (void*)fileContents,
          (tSize)(strlen(fileContents)+1));
        fprintf(stderr, "Wrote %d bytes\n", num_written_bytes);

        if (hdfsFlush(fs, userFile)) {
            fprintf(stderr, "Failed to 'flush' %s\n", userPath); 
            exit(-1);
        }
        fprintf(stderr, "Flushed %s successfully!\n", userPath); 

        hdfsCloseFile(fs, userFile);

        finfo = hdfsGetPathInfo(fs, userPath);
        fprintf(stderr, "hdfs new file user is correct: %s\n", ((result = (strcmp(finfo->mOwner, tuser))) != 0 ? "Failed!" : "Success!"));
        totalResult += result;
    }
    
    totalResult += (hdfsDisconnect(fs) != 0);

    if (totalResult != 0) {
        return -1;
    } else {
        return 0;
    }
}

/**
 * vim: ts=4: sw=4: et:
 */
