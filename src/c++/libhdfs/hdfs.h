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

#ifndef LIBHDFS_HDFS_H
#define LIBHDFS_HDFS_H

#include <sys/types.h>
#include <sys/stat.h>

#include <fcntl.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <errno.h>

#include <jni.h>

#ifndef O_RDONLY
#define O_RDONLY 1
#endif

#ifndef O_WRONLY 
#define O_WRONLY 2
#endif

#ifndef EINTERNAL
#define EINTERNAL 255 
#endif


/** All APIs set errno to meaningful values */
#ifdef __cplusplus
extern  "C" {
#endif

    /**
     * Some utility decls used in libhdfs.
     */

    typedef int32_t   tSize; /// size of data for read/write io ops 
    typedef time_t    tTime; /// time type
    typedef int64_t   tOffset;/// offset within the file
    typedef uint16_t  tPort; /// port
    typedef enum tObjectKind {
        kObjectKindFile = 'F',
        kObjectKindDirectory = 'D',
    } tObjectKind;


    /**
     * The C reflection of org.apache.org.hadoop.FileSystem .
     */
    typedef void* hdfsFS;

    
    /**
     * The C equivalent of org.apache.org.hadoop.FSData(Input|Output)Stream .
     */
    enum hdfsStreamType
    {
        UNINITIALIZED = 0,
        INPUT = 1,
        OUTPUT = 2,
    };

    
    /**
     * The 'file-handle' to a file in hdfs.
     */
    struct hdfsFile_internal {
        void* file;
        enum hdfsStreamType type;
    };
    typedef struct hdfsFile_internal* hdfsFile;
      

    /** 
     * hdfsConnect - Connect to a hdfs file system.
     * Connect to the hdfs.
     * @param host A string containing either a host name, or an ip address
     * of the namenode of a hdfs cluster. 'host' should be passed as NULL if
     * you want to connect to local filesystem. 'host' should be passed as
     * 'default' (and port as 0) to used the 'configured' filesystem
     * (hadoop-site/hadoop-default.xml).
     * @param port The port on which the server is listening.
     * @return Returns a handle to the filesystem or NULL on error.
     */
    hdfsFS hdfsConnect(const char* host, tPort port);


    /** 
     * hdfsDisconnect - Disconnect from the hdfs file system.
     * Disconnect from hdfs.
     * @param fs The configured filesystem handle.
     * @return Returns 0 on success, -1 on error.  
     */
    int hdfsDisconnect(hdfsFS fs);
        

    /** 
     * hdfsOpenFile - Open a hdfs file in given mode.
     * @param fs The configured filesystem handle.
     * @param path The full path to the file.
     * @param flags Either O_RDONLY or O_WRONLY, for read-only or write-only.
     * @param bufferSize Size of buffer for read/write - pass 0 if you want
     * to use the default configured values.
     * @param replication Block replication - pass 0 if you want to use
     * the default configured values.
     * @param blocksize Size of block - pass 0 if you want to use the
     * default configured values.
     * @return Returns the handle to the open file or NULL on error.
     */
    hdfsFile hdfsOpenFile(hdfsFS fs, const char* path, int flags,
                          int bufferSize, short replication, tSize blocksize);


    /** 
     * hdfsCloseFile - Close an open file. 
     * @param fs The configured filesystem handle.
     * @param file The file handle.
     * @return Returns 0 on success, -1 on error.  
     */
    int hdfsCloseFile(hdfsFS fs, hdfsFile file);


    /** 
     * hdfsExists - Checks if a given path exsits on the filesystem 
     * @param fs The configured filesystem handle.
     * @param path The path to look for
     * @return Returns 0 on success, -1 on error.  
     */
    int hdfsExists(hdfsFS fs, const char *path);


    /** 
     * hdfsSeek - Seek to given offset in file. 
     * This works only for files opened in read-only mode. 
     * @param fs The configured filesystem handle.
     * @param file The file handle.
     * @param desiredPos Offset into the file to seek into.
     * @return Returns 0 on success, -1 on error.  
     */
    int hdfsSeek(hdfsFS fs, hdfsFile file, tOffset desiredPos); 


    /** 
     * hdfsTell - Get the current offset in the file, in bytes.
     * @param fs The configured filesystem handle.
     * @param file The file handle.
     * @return Current offset, -1 on error.
     */
    tOffset hdfsTell(hdfsFS fs, hdfsFile file);


    /** 
     * hdfsRead - Read data from an open file.
     * @param fs The configured filesystem handle.
     * @param file The file handle.
     * @param buffer The buffer to copy read bytes into.
     * @param length The length of the buffer.
     * @return Returns the number of bytes actually read, possibly less
     * than than length;-1 on error.
     */
    tSize hdfsRead(hdfsFS fs, hdfsFile file, void* buffer, tSize length);


    /** 
     * hdfsPread - Positional read of data from an open file.
     * @param fs The configured filesystem handle.
     * @param file The file handle.
     * @param position Position from which to read
     * @param buffer The buffer to copy read bytes into.
     * @param length The length of the buffer.
     * @return Returns the number of bytes actually read, possibly less than
     * than length;-1 on error.
     */
    tSize hdfsPread(hdfsFS fs, hdfsFile file, tOffset position,
                    void* buffer, tSize length);


    /** 
     * hdfsWrite - Write data into an open file.
     * @param fs The configured filesystem handle.
     * @param file The file handle.
     * @param buffer The data.
     * @param length The no. of bytes to write. 
     * @return Returns the number of bytes written, -1 on error.
     */
    tSize hdfsWrite(hdfsFS fs, hdfsFile file, const void* buffer,
                    tSize length);


    /** 
     * hdfsWrite - Flush the data. 
     * @param fs The configured filesystem handle.
     * @param file The file handle.
     * @return Returns 0 on success, -1 on error. 
     */
    int hdfsFlush(hdfsFS fs, hdfsFile file);


    /**
     * hdfsAvailable - Number of bytes that can be read from this
     * input stream without blocking.
     * @param fs The configured filesystem handle.
     * @param file The file handle.
     * @return Returns available bytes; -1 on error. 
     */
    int hdfsAvailable(hdfsFS fs, hdfsFile file);


    /**
     * hdfsCopy - Copy file from one filesystem to another.
     * @param srcFS The handle to source filesystem.
     * @param src The path of source file. 
     * @param dstFS The handle to destination filesystem.
     * @param dst The path of destination file. 
     * @return Returns 0 on success, -1 on error. 
     */
    int hdfsCopy(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst);


    /**
     * hdfsMove - Move file from one filesystem to another.
     * @param srcFS The handle to source filesystem.
     * @param src The path of source file. 
     * @param dstFS The handle to destination filesystem.
     * @param dst The path of destination file. 
     * @return Returns 0 on success, -1 on error. 
     */
    int hdfsMove(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst);


    /**
     * hdfsDelete - Delete file. 
     * @param fs The configured filesystem handle.
     * @param path The path of the file. 
     * @return Returns 0 on success, -1 on error. 
     */
    int hdfsDelete(hdfsFS fs, const char* path);


    /**
     * hdfsRename - Rename file. 
     * @param fs The configured filesystem handle.
     * @param oldPath The path of the source file. 
     * @param newPath The path of the destination file. 
     * @return Returns 0 on success, -1 on error. 
     */
    int hdfsRename(hdfsFS fs, const char* oldPath, const char* newPath);


    /**
     * hdfsLock - Obtain a lock on the file.
     * @param fs The configured filesystem handle.
     * @param path The path of the file. 
     * @param shared Shared/exclusive lock-type. 
     * @return Returns 0 on success, -1 on error. 
     */
    int hdfsLock(hdfsFS fs, const char* path, int shared);


    /**
     * hdfsReleaseLock - Release the lock.
     * @param fs The configured filesystem handle.
     * @param path The path of the file. 
     * @return Returns 0 on success, -1 on error. 
     */
    int hdfsReleaseLock(hdfsFS fs, const char* path);


    /** 
     * hdfsGetWorkingDirectory - Get the current working directory for
     * the given filesystem.
     * @param fs The configured filesystem handle.
     * @param buffer The user-buffer to copy path of cwd into. 
     * @param bufferSize The length of user-buffer.
     * @return Returns buffer, NULL on error.
     */
    char* hdfsGetWorkingDirectory(hdfsFS fs, char *buffer, size_t bufferSize);


    /** 
     * hdfsSetWorkingDirectory - Set the working directory. All relative
     * paths will be resolved relative to it.
     * @param fs The configured filesystem handle.
     * @param path The path of the new 'cwd'. 
     * @return Returns 0 on success, -1 on error. 
     */
    int hdfsSetWorkingDirectory(hdfsFS fs, const char* path);


    /** 
     * hdfsCreateDirectory - Make the given file and all non-existent
     * parents into directories.
     * @param fs The configured filesystem handle.
     * @param path The path of the directory. 
     * @return Returns 0 on success, -1 on error. 
     */
    int hdfsCreateDirectory(hdfsFS fs, const char* path);


    /** 
     * hdfsFileInfo - Information about a file/directory.
     */
    typedef struct  {
        tObjectKind mKind;   /* file or directory */
        char *mName;         /* the name of the file */
        tTime mCreationTime; /* the creation time for the file*/
        tOffset mSize;       /* the size of the file in bytes */
        int replicaCount;    /* the count of replicas */
    } hdfsFileInfo;


    /** 
     * hdfsListDirectory - Get list of files/directories for a given
     * directory-path. freehdfsFileInfo should be called to deallocate memory. 
     * @param fs The configured filesystem handle.
     * @param path The path of the directory. 
     * @param numEntries Set to the number of files/directories in path.
     * @return Returns a dynamically-allocated array of hdfsFileInfo
     * objects; NULL on error.
     */
    hdfsFileInfo *hdfsListDirectory(hdfsFS fs, const char* path,
                                    int *numEntries);


    /** 
     * hdfsGetPathInfo - Get information about a path as a (dynamically
     * allocated) single hdfsFileInfo struct. freehdfsFileInfo should be
     * called when the pointer is no longer needed.
     * @param fs The configured filesystem handle.
     * @param path The path of the file. 
     * @return Returns a dynamically-allocated hdfsFileInfo object;
     * NULL on error.
     */
    hdfsFileInfo *hdfsGetPathInfo(hdfsFS fs, const char* path);


    /** 
     * hdfsFreeFileInfo - Free up the hdfsFileInfo array (including fields) 
     * @param hdfsFileInfo The array of dynamically-allocated hdfsFileInfo
     * objects.
     * @param numEntries The size of the array.
     */
    void hdfsFreeFileInfo(hdfsFileInfo *hdfsFileInfo, int numEntries);


    /** 
     * hdfsGetHosts - Get hostnames where a particular block (determined by
     * pos & blocksize) of a file is stored. The last element in the array
     * is NULL. Due to replication, a single block could be present on
     * multiple hosts.
     * @param fs The configured filesystem handle.
     * @param path The path of the file. 
     * @param start The start of the block.
     * @param length The length of the block.
     * @return Returns a dynamically-allocated 2-d array of blocks-hosts;
     * NULL on error.
     */
    char*** hdfsGetHosts(hdfsFS fs, const char* path, 
            tOffset start, tOffset length);


    /** 
     * hdfsFreeHosts - Free up the structure returned by hdfsGetHosts
     * @param hdfsFileInfo The array of dynamically-allocated hdfsFileInfo
     * objects.
     * @param numEntries The size of the array.
     */
    void hdfsFreeHosts(char ***blockHosts);


    /** 
     * hdfsGetDefaultBlockSize - Get the optimum blocksize.
     * @param fs The configured filesystem handle.
     * @return Returns the blocksize; -1 on error. 
     */
    tOffset hdfsGetDefaultBlockSize(hdfsFS fs);


    /** 
     * hdfsGetCapacity - Return the raw capacity of the filesystem.  
     * @param fs The configured filesystem handle.
     * @return Returns the raw-capacity; -1 on error. 
     */
    tOffset hdfsGetCapacity(hdfsFS fs);


    /** 
     * hdfsGetUsed - Return the total raw size of all files in the filesystem.
     * @param fs The configured filesystem handle.
     * @return Returns the total-size; -1 on error. 
     */
    tOffset hdfsGetUsed(hdfsFS fs);
    
#ifdef __cplusplus
}
#endif

#endif /*LIBHDFS_HDFS_H*/

/**
 * vim: ts=4: sw=4: et
 */
