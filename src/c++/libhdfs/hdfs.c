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
#include "hdfsJniHelper.h"


/* Some frequently used Java paths */
#define HADOOP_CONF     "org/apache/hadoop/conf/Configuration"
#define HADOOP_PATH     "org/apache/hadoop/fs/Path"
#define HADOOP_LOCALFS  "org/apache/hadoop/fs/LocalFileSystem"
#define HADOOP_FS       "org/apache/hadoop/fs/FileSystem"
#define HADOOP_DFS      "org/apache/hadoop/dfs/DistributedFileSystem"
#define HADOOP_ISTRM    "org/apache/hadoop/fs/FSDataInputStream"
#define HADOOP_OSTRM    "org/apache/hadoop/fs/FSDataOutputStream"
#define JAVA_NET_ISA    "java/net/InetSocketAddress"


/* Macros for constructing method signatures */
#define JPARAM(X)           "L" X ";"
#define JARRPARAM(X)        "[L" X ";"
#define JMETHOD1(X, R)      "(" X ")" R
#define JMETHOD2(X, Y, R)   "(" X Y ")" R


/**
 * hdfsJniEnv: A wrapper struct to be used as 'value'
 * while saving thread -> JNIEnv* mappings
 */
typedef struct
{
    JNIEnv* env;
} hdfsJniEnv;



/**
 * Helper function to destroy a local reference of java.lang.Object
 * @param env: The JNIEnv pointer. 
 * @param jFile: The local reference of java.lang.Object object
 * @return None.
 */
static void destroyLocalReference(JNIEnv *env, jobject jObject)
{
  (*env)->DeleteLocalRef(env, jObject);
}


/**
 * Helper function to create a org.apache.hadoop.fs.Path object.
 * @param env: The JNIEnv pointer. 
 * @param path: The file-path for which to construct org.apache.hadoop.fs.Path
 * object.
 * @return Returns a jobject on success and NULL on error.
 */
static jobject constructNewObjectOfPath(JNIEnv *env, const char *path)
{
    //Construct a java.lang.String object
    jstring jPathString = (*env)->NewStringUTF(env, path); 

    //Construct the org.apache.hadoop.fs.Path object
    jobject jPath =
        constructNewObjectOfClass(env, "org/apache/hadoop/fs/Path",
                                  "(Ljava/lang/String;)V", jPathString);
    if (jPath == NULL) {
        fprintf(stderr, "Can't construct instance of class "
                "org.apache.hadoop.fs.Path for %s\n", path);
        errno = EINTERNAL;
        return NULL;
    }

    // Destroy the local reference to the java.lang.String object
    destroyLocalReference(env, jPathString);

    return jPath;
}






hdfsFS hdfsConnect(const char* host, tPort port)
{
    // JAVA EQUIVALENT:
    //  FileSystem fs = FileSystem.get(new Configuration());
    //  return fs;

    JNIEnv *env = 0;
    jobject jConfiguration;
    jobject jFS = NULL;
    jvalue  jVal;

    //Get the JNIEnv* corresponding to current thread
    env = getJNIEnv();

    //Create the org.apache.hadoop.conf.Configuration object
    jConfiguration =
        constructNewObjectOfClass(env, HADOOP_CONF, "()V");

    if (jConfiguration == NULL) {
        fprintf(stderr, "Can't construct instance of class "
                "org.apache.hadoop.conf.Configuration\n");
        errno = EINTERNAL;
        return NULL;
    }
 
    //Check what type of FileSystem the caller wants...
    if (host == NULL) {
        //fs = new LocalFileSystem(conf);
        jFS = constructNewObjectOfClass(env, HADOOP_LOCALFS,
                                        JMETHOD1(JPARAM(HADOOP_CONF), "V"),
                                        jConfiguration);
        if (jFS == NULL) {
            errno = EINTERNAL;
            goto done;
        }
    }
    else if (!strcmp(host, "default") && port == 0) {
        //fs = FileSystem::get(conf); 
        if (invokeMethod(env, &jVal, STATIC, NULL,
                         HADOOP_FS, "get",
                         JMETHOD1(JPARAM(HADOOP_CONF),
                                  JPARAM(HADOOP_FS)),
                         jConfiguration) != 0) {
            fprintf(stderr, "Call to org.apache.hadoop.fs."
                    "FileSystem::get failed!\n");
            errno = EINTERNAL;
            goto done;
        }
        jFS = jVal.l;
    }
    else {
     //fs = new DistributedFileSystem(new InetSocketAddress(host, port), conf)
        jstring jHostName = (*env)->NewStringUTF(env, host);
        jobject jNameNodeAddr = 
            constructNewObjectOfClass(env, JAVA_NET_ISA,
                                      "(Ljava/lang/String;I)V",
                                      jHostName, port);

        destroyLocalReference(env, jHostName);
        if (jNameNodeAddr == NULL) {
            errno = EINTERNAL;
            goto done;
        }
    
        jFS = constructNewObjectOfClass(env, HADOOP_DFS,
                                        JMETHOD2(JPARAM(JAVA_NET_ISA),
                                                 JPARAM(HADOOP_CONF), "V"),
                                        jNameNodeAddr, jConfiguration);

        destroyLocalReference(env, jNameNodeAddr);
        if (jFS == NULL) {
            errno = EINTERNAL;
            goto done;
        }
    }

  done:
    
    //Release unnecessary local references
    destroyLocalReference(env, jConfiguration);

    /* Create a global reference for this fs */
    jobject gFsRef = NULL;

    if (jFS) {
        gFsRef = (*env)->NewGlobalRef(env, jFS);
        destroyLocalReference(env, jFS);
    }

    return gFsRef;
}



int hdfsDisconnect(hdfsFS fs)
{
    // JAVA EQUIVALENT:
    //  fs.close()

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    //Parameters
    jobject jFS = (jobject)fs;

    //Sanity check
    if (fs == NULL) {
        errno = EBADF;
        return -1;
    }

    if (invokeMethod(env, NULL, INSTANCE, jFS, HADOOP_FS,
                     "close", "()V") != 0) {
        fprintf(stderr, "Call to FileSystem::close failed!\n"); 
        errno = EINTERNAL;
        return -1;
    }

    //Release unnecessary references
    (*env)->DeleteGlobalRef(env, fs);

    return 0;
}



hdfsFile hdfsOpenFile(hdfsFS fs, const char* path, int flags, 
                      int bufferSize, short replication, tSize blockSize)
{
    /*
      JAVA EQUIVALENT:
       File f = new File(path);
       FSData{Input|Output}Stream f{is|os} = fs.create(f);
       return f{is|os};
    */

    /* Get the JNIEnv* corresponding to current thread */
    JNIEnv* env = getJNIEnv();

    jobject jFS = (jobject)fs;

    /* The hadoop java api/signature */
    const char* method = (flags == O_RDONLY) ? "open" : "create";
    const char* signature = (flags == O_RDONLY) ?
        JMETHOD2(JPARAM(HADOOP_PATH), "I", JPARAM(HADOOP_ISTRM)) :
        JMETHOD2(JPARAM(HADOOP_PATH), "ZISJ", JPARAM(HADOOP_OSTRM));

    /* Return value */
    hdfsFile file = NULL;

    /* Create an object of org.apache.hadoop.fs.Path */
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return NULL; 
    }

    /* Get the Configuration object from the FileSystem object */
    jvalue  jVal;
    jobject jConfiguration = NULL;
    if (invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                     "getConf", JMETHOD1("", JPARAM(HADOOP_CONF))) != 0) {
        fprintf(stderr, "Failed to get configuration object from "
                "filesystem\n");
        errno = EINTERNAL;
        destroyLocalReference(env, jPath);
        return NULL;
    }
    jConfiguration = jVal.l;

    jint jBufferSize = bufferSize;
    jshort jReplication = replication;
    jlong jBlockSize = blockSize;
    jstring jStrBufferSize = (*env)->NewStringUTF(env, "io.file.buffer.size"); 
    jstring jStrReplication = (*env)->NewStringUTF(env, "dfs.replication");
    jstring jStrBlockSize = (*env)->NewStringUTF(env, "dfs.block.size");


    //bufferSize
    if (!bufferSize) {
        if (invokeMethod(env, &jVal, INSTANCE, jConfiguration, 
                         HADOOP_CONF, "getInt", "(Ljava/lang/String;I)I",
                         jStrBufferSize, 4096) != 0) {
            fprintf(stderr, "Call to org.apache.hadoop.conf."
                    "Configuration::getInt failed!\n");
            errno = EINTERNAL;
            goto done;
        }
        jBufferSize = jVal.i;
    }

    if (flags == O_WRONLY) {
        //replication

        if (!replication) {
            if (invokeMethod(env, &jVal, INSTANCE, jConfiguration, 
                             HADOOP_CONF, "getInt", "(Ljava/lang/String;I)I",
                             jStrReplication, 1) != 0) {
                fprintf(stderr, "Call to org.apache.hadoop.conf."
                        "Configuration::getInt failed!\n");
                errno = EINTERNAL;
                goto done;
            }
            jReplication = jVal.i;
        }
        
        //blockSize
        if (!blockSize) {
            if (invokeMethod(env, &jVal, INSTANCE, jConfiguration, 
                             HADOOP_CONF, "getLong", "(Ljava/lang/String;J)J",
                             jStrBlockSize, 67108864)) {
                fprintf(stderr, "Call to org.apache.hadoop.fs."
                        "FileSystem::%s(%s) failed!\n", method, signature);
                errno = EINTERNAL;
                goto done;
            }
            jBlockSize = jVal.j;
        }
    }
 
    /* Create and return either the FSDataInputStream or
       FSDataOutputStream references jobject jStream */

    if (flags == O_RDONLY) {
        if (invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                         method, signature, jPath, jBufferSize)) {
            fprintf(stderr, "Call to org.apache.hadoop.fs."
                    "FileSystem::%s(%s) failed!\n", method, signature);
            errno = EINTERNAL;
            goto done;
        }
    }
    else {
        jboolean jOverWrite = 1;
        if (invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                         method, signature, jPath, jOverWrite,
                         jBufferSize, jReplication, jBlockSize)) {
            fprintf(stderr, "Call to org.apache.hadoop.fs."
                    "FileSystem::%s(%s) failed!\n", method, signature);
            errno = EINTERNAL;
            goto done;
        }
    }
  
    file = malloc(sizeof(struct hdfsFile_internal));
    if (!file) {
        errno = ENOMEM;
        return NULL;
    }
    file->file = (*env)->NewGlobalRef(env, jVal.l);
    file->type = ((flags == O_RDONLY) ? INPUT : OUTPUT);

    destroyLocalReference(env, jVal.l);

    done:

    //Delete unnecessary local references
    destroyLocalReference(env, jStrBufferSize);
    destroyLocalReference(env, jStrReplication);
    destroyLocalReference(env, jStrBlockSize);
    destroyLocalReference(env, jConfiguration); 
    destroyLocalReference(env, jPath); 

    return file;
}



int hdfsCloseFile(hdfsFS fs, hdfsFile file)
{
    // JAVA EQUIVALENT:
    //  file.close 

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    //Parameters
    jobject jStream = (jobject)(file ? file->file : NULL);

    //Sanity check
    if (!file || file->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }

    //The interface whose 'close' method to be called
    const char* interface = (file->type == INPUT) ? 
        HADOOP_ISTRM : HADOOP_OSTRM;
  
    if (invokeMethod(env, NULL, INSTANCE, jStream, interface,
                     "close", "()V") != 0) {
        fprintf(stderr, "Call to %s::close failed!\n", interface); 
        errno = EINTERNAL;
        return -1;
    }

    //De-allocate memory
    free(file);
    (*env)->DeleteGlobalRef(env, jStream);

    return 0;
}



int hdfsExists(hdfsFS fs, const char *path)
{
    JNIEnv *env = getJNIEnv();
    jobject jPath = constructNewObjectOfPath(env, path);
    jvalue  jVal;
    jobject jFS = (jobject)fs;

    if (jPath == NULL) {
        return -1;
    }

    if (invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                     "exists", JMETHOD1(JPARAM(HADOOP_PATH), "Z"),
                     jPath) != 0) {
        fprintf(stderr, "Call to org.apache.hadoop.fs."
                "FileSystem::exists failed!\n"); 
        errno = EINTERNAL;
        return -1;
    }

    return jVal.z ? 0 : -1;
}



tSize hdfsRead(hdfsFS fs, hdfsFile f, void* buffer, tSize length)
{
    // JAVA EQUIVALENT:
    //  byte [] bR = new byte[length];
    //  fis.read(bR);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    //Parameters
    jobject jInputStream = (jobject)(f ? f->file : NULL);

    jbyteArray jbRarray;
    jint noReadBytes = 0;
    jvalue jVal;

    //Sanity check
    if (!f || f->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }

    //Error checking... make sure that this file is 'readable'
    if (f->type != INPUT) {
        fprintf(stderr, "Cannot read from a non-InputStream object!\n");
        errno = EINVAL;
        return -1;
    }

    //Read the requisite bytes
    jbRarray = (*env)->NewByteArray(env, length);
    if (invokeMethod(env, &jVal, INSTANCE, jInputStream, HADOOP_ISTRM,
                     "read", "([B)I", jbRarray) != 0) {
        fprintf(stderr, "Call to org.apache.hadoop.fs."
                "FSDataInputStream::read failed!\n");
        errno = EINTERNAL;
        noReadBytes = -1;
    }
    else {
        noReadBytes = jVal.i;
        if (noReadBytes > 0) {
            (*env)->GetByteArrayRegion(env, jbRarray, 0, noReadBytes, buffer);
        }
        //This is a valid case: there aren't any bytes left to read!
        errno = 0;
    }

    destroyLocalReference(env, jbRarray);

    return noReadBytes;
}


  
tSize hdfsPread(hdfsFS fs, hdfsFile f, tOffset position,
                void* buffer, tSize length)
{
    // JAVA EQUIVALENT:
    //  byte [] bR = new byte[length];
    //  fis.read(pos, bR, 0, length);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    //Parameters
    jobject jInputStream = (jobject)(f ? f->file : NULL);

    jbyteArray jbRarray;
    jint noReadBytes = 0;
    jvalue jVal;

    //Sanity check
    if (!f || f->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }

    //Error checking... make sure that this file is 'readable'
    if (f->type != INPUT) {
        fprintf(stderr, "Cannot read from a non-InputStream object!\n");
        errno = EINVAL;
        return -1;
    }

    //Read the requisite bytes
    jbRarray = (*env)->NewByteArray(env, length);
    if (invokeMethod(env, &jVal, INSTANCE, jInputStream, HADOOP_ISTRM,
                     "read", "(J[BII)I", position, jbRarray, 0, length) != 0) {
        fprintf(stderr, "Call to org.apache.hadoop.fs."
                "FSDataInputStream::read failed!\n");
        errno = EINTERNAL;
        noReadBytes = -1;
    }
    else {
        noReadBytes = jVal.i;
        if (noReadBytes > 0) {
            (*env)->GetByteArrayRegion(env, jbRarray, 0, noReadBytes, buffer);
        }
        //This is a valid case: there aren't any bytes left to read!
        errno = 0;
    }
    destroyLocalReference(env, jbRarray);

    return noReadBytes;
}



tSize hdfsWrite(hdfsFS fs, hdfsFile f, const void* buffer, tSize length)
{
    // JAVA EQUIVALENT
    // byte b[] = str.getBytes();
    // fso.write(b);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    //Parameters
    jobject jOutputStream = (jobject)(f ? f->file : 0);
    jbyteArray jbWarray;

    //Sanity check
    if (!f || f->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }
    
    if (length < 0) {
    	errno = EINVAL;
    	return -1;
    }

    //Error checking... make sure that this file is 'writable'
    if (f->type != OUTPUT) {
        fprintf(stderr, "Cannot write into a non-OutputStream object!\n");
        errno = EINVAL;
        return -1;
    }

    // 'length' equals 'zero' is a valid use-case according to Posix!
    if (length != 0) {
        //Write the requisite bytes into the file
        jbWarray = (*env)->NewByteArray(env, length);
        (*env)->SetByteArrayRegion(env, jbWarray, 0, length, buffer);
        if (invokeMethod(env, NULL, INSTANCE, jOutputStream,
                         HADOOP_OSTRM, "write",
                         "([B)V", jbWarray) != 0) {
            fprintf(stderr, "Call to org.apache.hadoop.fs."
                    "FSDataOutputStream::write failed!\n");
            errno = EINTERNAL;
            length = -1;
        }
        destroyLocalReference(env, jbWarray);
    }

    //Return no. of bytes succesfully written (libc way)
    //i.e. 'length' itself! ;-)
    return length;
}



int hdfsSeek(hdfsFS fs, hdfsFile f, tOffset desiredPos) 
{
    // JAVA EQUIVALENT
    //  fis.seek(pos);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    //Parameters
    jobject jInputStream = (jobject)(f ? f->file : 0);

    //Sanity check
    if (!f || f->type != INPUT) {
        errno = EBADF;
        return -1;
    }

    if (invokeMethod(env, NULL, INSTANCE, jInputStream, HADOOP_ISTRM,
                     "seek", "(J)V", desiredPos) != 0) {
        fprintf(stderr, "Call to org.apache.hadoop.fs."
                "FSDataInputStream::seek failed!\n");
        errno = EINTERNAL;
        return -1;
    }

    return 0;
}



tOffset hdfsTell(hdfsFS fs, hdfsFile f)
{
    // JAVA EQUIVALENT
    //  pos = f.getPos();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    //Parameters
    jobject jStream = (jobject)(f ? f->file : 0);

    //Sanity check
    if (!f || f->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }

    const char* interface = (f->type == INPUT) ?
        HADOOP_ISTRM : HADOOP_OSTRM;

    jlong currentPos  = -1;
    jvalue jVal;
    if (invokeMethod(env, &jVal, INSTANCE, jStream,
                     interface, "getPos", "()J") != 0) {
        fprintf(stderr, "Call to org.apache.hadoop.fs."
                "FSDataInputStream::getPos failed!\n");
        errno = EINTERNAL;
        return -1;
    }
    currentPos = jVal.j;

    return (tOffset)currentPos;
}



int hdfsFlush(hdfsFS fs, hdfsFile f) 
{
    // JAVA EQUIVALENT
    //  fos.flush();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    //Parameters
    jobject jOutputStream = (jobject)(f ? f->file : 0);

    //Sanity check
    if (!f || f->type != OUTPUT) {
        errno = EBADF;
        return -1;
    }

    if (invokeMethod(env, NULL, INSTANCE, jOutputStream, 
                     HADOOP_OSTRM, "flush", "()V") != 0) {
        fprintf(stderr, "Call to org.apache.hadoop.fs."
                "FSDataInputStream::flush failed!\n");
        errno = EINTERNAL;
        return -1;
    }

    return 0;
}



int hdfsAvailable(hdfsFS fs, hdfsFile f)
{
    // JAVA EQUIVALENT
    //  fis.available();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    //Parameters
    jobject jInputStream = (jobject)(f ? f->file : 0);

    //Sanity check
    if (!f || f->type != INPUT) {
        errno = EBADF;
        return -1;
    }

    jint available = -1;
    jvalue jVal;
    if (invokeMethod(env, &jVal, INSTANCE, jInputStream, 
                     HADOOP_ISTRM, "available", "()I") != 0) {
        fprintf(stderr, "Call to org.apache.hadoop.fs."
                "FSDataInputStream::available failed!\n");
        errno = EINTERNAL;
        return -1;
    }
    available = jVal.i;

    return available;
}



int hdfsCopy(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst)
{
    //JAVA EQUIVALENT
    //  FileUtil::copy(srcFS, srcPath, dstFS, dstPath,
    //                 deleteSource = false, conf)

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    //Parameters
    jobject jSrcFS = (jobject)srcFS;
    jobject jDstFS = (jobject)dstFS;
    jobject jSrcPath = NULL;
    jobject jDstPath = NULL;

    jSrcPath = constructNewObjectOfPath(env, src);
    if (jSrcPath == NULL) {
        return -1;
    }

    jDstPath = constructNewObjectOfPath(env, dst);
    if (jDstPath == NULL) {
        destroyLocalReference(env, jSrcPath);
        return -1;
    }

    int retval = 0;

    //Create the org.apache.hadoop.conf.Configuration object
    jobject jConfiguration =
        constructNewObjectOfClass(env, HADOOP_CONF, "()V");
    if (jConfiguration == NULL) {
        fprintf(stderr, "Can't construct instance of class "
                "org.apache.hadoop.conf.Configuration\n");
        errno = EINTERNAL;
        destroyLocalReference(env, jSrcPath);
        destroyLocalReference(env, jDstPath);
        return -1;
    }

    //FileUtil::copy
    jboolean deleteSource = 0; //Only copy
    jvalue jVal;
    if (invokeMethod(env, &jVal, STATIC, 
                     NULL, "org/apache/hadoop/fs/FileUtil", "copy",
                     "(Lorg/apache/hadoop/fs/FileSystem;Lorg/apache/hadoop/fs/Path;Lorg/apache/hadoop/fs/FileSystem;Lorg/apache/hadoop/fs/Path;ZLorg/apache/hadoop/conf/Configuration;)Z",
                     jSrcFS, jSrcPath, jDstFS, jDstPath, deleteSource, 
                     jConfiguration) != 0) {
        fprintf(stderr, "Call to org.apache.hadoop.fs."
                "FileUtil::copy failed!\n");
        errno = EINTERNAL;
        retval = -1;
        goto done;
    }

    done:

    //Delete unnecessary local references
    destroyLocalReference(env, jConfiguration);
    destroyLocalReference(env, jSrcPath);
    destroyLocalReference(env, jDstPath);
  
    return retval;
}



int hdfsMove(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst)
{
    //JAVA EQUIVALENT
    //  FileUtil::copy(srcFS, srcPath, dstFS, dstPath,
    //                 deleteSource = true, conf)

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    //Parameters
    jobject jSrcFS = (jobject)srcFS;
    jobject jDstFS = (jobject)dstFS;

    jobject jSrcPath = NULL;
    jobject jDstPath = NULL;

    jSrcPath = constructNewObjectOfPath(env, src);
    if (jSrcPath == NULL) {
        return -1;
    }

    jDstPath = constructNewObjectOfPath(env, dst);
    if (jDstPath == NULL) {
        destroyLocalReference(env, jSrcPath);
        return -1;
    }

    int retval = 0;

    //Create the org.apache.hadoop.conf.Configuration object
    jobject jConfiguration =
        constructNewObjectOfClass(env, HADOOP_CONF, "()V");
    if (jConfiguration == NULL) {
        fprintf(stderr, "Can't construct instance of class "
                "org.apache.hadoop.conf.Configuration\n");
        errno = EINTERNAL;
        destroyLocalReference(env, jSrcPath);
        destroyLocalReference(env, jDstPath);
        return -1;
    }

    //FileUtil::copy
    jboolean deleteSource = 1; //Delete src after copy
    jvalue jVal;
    if (invokeMethod(env, &jVal, STATIC, NULL,
                     "org/apache/hadoop/fs/FileUtil", "copy",
                "(Lorg/apache/hadoop/fs/FileSystem;Lorg/apache/hadoop/fs/Path;Lorg/apache/hadoop/fs/FileSystem;Lorg/apache/hadoop/fs/Path;ZLorg/apache/hadoop/conf/Configuration;)Z",
                     jSrcFS, jSrcPath, jDstFS, jDstPath, deleteSource, 
                     jConfiguration) != 0) {
        fprintf(stderr, "Call to org.apache.hadoop.fs."
                "FileUtil::copy(move) failed!\n");
        errno = EINTERNAL;
        retval = -1;
        goto done;
    }

    done:

    //Delete unnecessary local references
    destroyLocalReference(env, jConfiguration);
    destroyLocalReference(env, jSrcPath);
    destroyLocalReference(env, jDstPath);
  
    return retval;
}



int hdfsDelete(hdfsFS fs, const char* path)
{
    // JAVA EQUIVALENT:
    //  File f = new File(path);
    //  bool retval = fs.delete(f);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    jobject jFS = (jobject)fs;

    //Create an object of java.io.File
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return -1;
    }

    //Delete the file
    jvalue jVal;
    if (invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                     "delete", "(Lorg/apache/hadoop/fs/Path;)Z",
                     jPath) != 0) {
        fprintf(stderr, "Call to org.apache.hadoop.fs."
                "FileSystem::delete failed!\n");
        errno = EINTERNAL;
        return -1;
    }

    //Delete unnecessary local references
    destroyLocalReference(env, jPath);

    return (jVal.z) ? 0 : -1;
}



int hdfsRename(hdfsFS fs, const char* oldPath, const char* newPath)
{
    // JAVA EQUIVALENT:
    //  Path old = new Path(oldPath);
    //  Path new = new Path(newPath);
    //  fs.rename(old, new);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    jobject jFS = (jobject)fs;

    //Create objects of org.apache.hadoop.fs.Path
    jobject jOldPath = NULL;
    jobject jNewPath = NULL;

    jOldPath = constructNewObjectOfPath(env, oldPath);
    if (jOldPath == NULL) {
        return -1;
    }

    jNewPath = constructNewObjectOfPath(env, newPath);
    if (jNewPath == NULL) {
        destroyLocalReference(env, jOldPath);
        return -1;
    }

    //Rename the file
    jvalue jVal;
    if (invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS, "rename",
                     JMETHOD2(JPARAM(HADOOP_PATH), JPARAM(HADOOP_PATH), "Z"),
                     jOldPath, jNewPath) != 0) {
        fprintf(stderr, "Call to org.apache.hadoop.fs."
                "FileSystem::rename failed!\n");
        errno = EINTERNAL;
        return -1;
    }

    //Delete unnecessary local references
    destroyLocalReference(env, jOldPath);
    destroyLocalReference(env, jNewPath);

    return (jVal.z) ? 0 : -1;
}



int hdfsLock(hdfsFS fs, const char* path, int shared)
{
    // JAVA EQUIVALENT:
    //  Path p = new Path(path);
    //  fs.lock(p);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    //Parameters
    jobject jFS = (jobject)fs;
    jboolean jb_shared = shared;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return -1;
    }

    //Lock the file
    int retval = 0;
    if (invokeMethod(env, NULL, INSTANCE, jFS, HADOOP_FS,
                     "lock", "(Lorg/apache/hadoop/fs/Path;Z)V",
                     jPath, jb_shared) != 0) {
        fprintf(stderr, "Call to org.apache.fs.FileSystem::lock failed!\n");
        errno = EINTERNAL;
        retval = -1;
    }

    //Delete unnecessary local references
    destroyLocalReference(env, jPath);

    return retval;
}



int hdfsReleaseLock(hdfsFS fs, const char* path)
{
    // JAVA EQUIVALENT:
    //  Path f = new Path(path);
    //  fs.release(f);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    jobject jFS = (jobject)fs;

    //Create an object of java.io.File
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return -1;
    }

    //Release the lock on the file
    int retval = 0;
    if (invokeMethod(env, NULL, INSTANCE, jFS, HADOOP_FS, "release",
                     "(Lorg/apache/hadoop/fs/Path;)V", jPath) != 0) {
        fprintf(stderr, "Call to org.apache.hadoop.fs.FileSystem::"
                "release failed!\n");
        errno = EINTERNAL;
        retval = -1;
        goto done;
    }

    done:

    //Delete unnecessary local references
    destroyLocalReference(env, jPath);

    return retval;
}



char* hdfsGetWorkingDirectory(hdfsFS fs, char* buffer, size_t bufferSize)
{
    // JAVA EQUIVALENT:
    //  Path p = fs.getWorkingDirectory(); 
    //  return p.toString()

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    jobject jFS = (jobject)fs;
    jobject jPath = NULL;
    jvalue jVal;

    //FileSystem::getWorkingDirectory()
    if (invokeMethod(env, &jVal, INSTANCE, jFS,
                     HADOOP_FS, "getWorkingDirectory",
                     "()Lorg/apache/hadoop/fs/Path;") != 0 ||
        jVal.l == NULL) {
        fprintf(stderr, "Call to FileSystem::getWorkingDirectory failed!\n");
        errno = EINTERNAL;
        return NULL;
    }
    jPath = jVal.l;

    //Path::toString()
    jstring jPathString;
    if (invokeMethod(env, &jVal, INSTANCE, jPath, 
                     "org/apache/hadoop/fs/Path", "toString",
                     "()Ljava/lang/String;") != 0) { 
        fprintf(stderr, "Call to Path::toString failed!\n");
        errno = EINTERNAL;
        destroyLocalReference(env, jPath);
        return NULL;
    }
    jPathString = jVal.l;

    const char *jPathChars = (const char*)
        ((*env)->GetStringUTFChars(env, jPathString, NULL));

    //Copy to user-provided buffer
    strncpy(buffer, jPathChars, bufferSize);

    //Delete unnecessary local references
    (*env)->ReleaseStringUTFChars(env, jPathString, jPathChars);

    destroyLocalReference(env, jPathString);
    destroyLocalReference(env, jPath);

    return buffer;
}



int hdfsSetWorkingDirectory(hdfsFS fs, const char* path)
{
    // JAVA EQUIVALENT:
    //  fs.setWorkingDirectory(Path(path)); 

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    jobject jFS = (jobject)fs;
    int retval = 0;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return -1;
    }

    //FileSystem::setWorkingDirectory()
    if (invokeMethod(env, NULL, INSTANCE, jFS, HADOOP_FS,
                     "setWorkingDirectory", 
                     "(Lorg/apache/hadoop/fs/Path;)V", jPath) != 0) {
        fprintf(stderr, "Call to FileSystem::setWorkingDirectory failed!\n");
        errno = EINTERNAL;
        retval = -1;
    }

    //Delete unnecessary local references
    destroyLocalReference(env, jPath);

    return retval;
}



int hdfsCreateDirectory(hdfsFS fs, const char* path)
{
    // JAVA EQUIVALENT:
    //  fs.mkdirs(new Path(path));

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    jobject jFS = (jobject)fs;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return -1;
    }

    //Create the directory
    jvalue jVal;
    jVal.z = 0;
    if (invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                     "mkdirs", "(Lorg/apache/hadoop/fs/Path;)Z",
                     jPath) != 0) {
        fprintf(stderr, "Call to org.apache.hadoop.fs.FileSystem::"
                "mkdirs failed!\n");
        errno = EINTERNAL;
        goto done;
    }

 done:

    //Delete unnecessary local references
    destroyLocalReference(env, jPath);

    return (jVal.z) ? 0 : -1;
}



char***
hdfsGetHosts(hdfsFS fs, const char* path, tOffset start, tOffset length)
{
    // JAVA EQUIVALENT:
    //  fs.getFileCacheHints(new Path(path), start, length);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    jobject jFS = (jobject)fs;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return NULL;
    }

    //org.apache.hadoop.fs.FileSystem::getFileCacheHints
    char*** blockHosts = NULL;
    jobjectArray jFileCacheHints;
    jvalue jVal;
    if (invokeMethod(env, &jVal, INSTANCE, jFS,
                     HADOOP_FS, "getFileCacheHints", 
                     "(Lorg/apache/hadoop/fs/Path;JJ)[[Ljava/lang/String;",
                     jPath, start, length) != 0) {
        fprintf(stderr, "Call to org.apache.hadoop.fs."
                "FileSystem::getFileCacheHints failed!\n");
        errno = EINTERNAL;
        destroyLocalReference(env, jPath);
        return NULL;
    }
    jFileCacheHints = jVal.l;

    //Figure out no of entries in jFileCacheHints 
    //Allocate memory and add NULL at the end
    jsize jNumFileBlocks = (*env)->GetArrayLength(env, jFileCacheHints);

    blockHosts = malloc(sizeof(char**) * (jNumFileBlocks+1));
    if (blockHosts == NULL) {
        errno = ENOMEM;
        goto done;
    }
    blockHosts[jNumFileBlocks] = NULL;
    if (jNumFileBlocks == 0) {
        errno = 0;
        goto done;
    }

    //Now parse each block to get hostnames
    int i = 0;
    for (i=0; i < jNumFileBlocks; ++i) {
        jobjectArray jFileBlockHosts =
            (*env)->GetObjectArrayElement(env, jFileCacheHints, i);

        //Figure out no of entries in jFileCacheHints 
        //Allocate memory and add NULL at the end
        jsize jNumBlockHosts = (*env)->GetArrayLength(env, jFileBlockHosts);
        blockHosts[i] = malloc(sizeof(char*) * (jNumBlockHosts+1));
        if (blockHosts[i] == NULL) {
            int x = 0;
            for (x=0; x < i; ++x) {
                free(blockHosts[x]);
            }
            free(blockHosts);
            errno = ENOMEM;
            goto done;
        }
        blockHosts[i][jNumBlockHosts] = NULL;

        //Now parse each hostname
        int j = 0;
        const char *hostName;
        for (j=0; j < jNumBlockHosts; ++j) {
            jstring jHost =
                (*env)->GetObjectArrayElement(env, jFileBlockHosts, j);
           
            hostName =
                (const char*)((*env)->GetStringUTFChars(env, jHost, NULL));
            blockHosts[i][j] = strdup(hostName);

            (*env)->ReleaseStringUTFChars(env, jHost, hostName);
            destroyLocalReference(env, jHost);
        }

        destroyLocalReference(env, jFileBlockHosts);
    }
  
    done:

    //Delete unnecessary local references
    destroyLocalReference(env, jPath);
    destroyLocalReference(env, jFileCacheHints);

    return blockHosts;
}


void hdfsFreeHosts(char ***blockHosts)
{
    int i, j;
    for (i=0; blockHosts[i]; i++) {
        for (j=0; blockHosts[i][j]; j++) {
            free(blockHosts[i][j]);
        }
        free(blockHosts[i]);
    }
    free(blockHosts);
}


tOffset hdfsGetDefaultBlockSize(hdfsFS fs)
{
    // JAVA EQUIVALENT:
    //  fs.getDefaultBlockSize();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    jobject jFS = (jobject)fs;

    //FileSystem::getDefaultBlockSize()
    tOffset blockSize = -1;
    jvalue jVal;
    if (invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                     "getDefaultBlockSize", "()J") != 0) {
        fprintf(stderr, "Call to org.apache.hadoop.fs."
                "FileSystem::getDefaultBlockSize failed!\n");
        errno = EINTERNAL;
        return -1;
    }
    blockSize = jVal.j;

    return blockSize;
}



tOffset hdfsGetCapacity(hdfsFS fs)
{
    // JAVA EQUIVALENT:
    //  fs.getRawCapacity();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    jobject jFS = (jobject)fs;

    if (!((*env)->IsInstanceOf(env, jFS, 
                               globalClassReference(HADOOP_DFS, env)))) {
        fprintf(stderr, "hdfsGetCapacity works only on a "
                "DistributedFileSystem!\n");
        return -1;
    }

    //FileSystem::getRawCapacity()
    jvalue  jVal;
    if (invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_DFS,
                     "getRawCapacity", "()J") != 0) {
        fprintf(stderr, "Call to org.apache.hadoop.fs."
                "FileSystem::getRawCapacity failed!\n");
        errno = EINTERNAL;
        return -1;
    }

    return jVal.j;
}


  
tOffset hdfsGetUsed(hdfsFS fs)
{
    // JAVA EQUIVALENT:
    //  fs.getRawUsed();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    jobject jFS = (jobject)fs;

    if (!((*env)->IsInstanceOf(env, jFS, 
                               globalClassReference(HADOOP_DFS, env)))) {
        fprintf(stderr, "hdfsGetUsed works only on a "
                "DistributedFileSystem!\n");
        return -1;
    }

    //FileSystem::getRawUsed()
    jvalue jVal;
    if (invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_DFS,
                     "getRawUsed", "()J") != 0) {
        fprintf(stderr, "Call to org.apache.hadoop.fs."
                "FileSystem::getRawUsed failed!\n");
        errno = EINTERNAL;
        return -1;
    }

    return jVal.j;
}


 
static int
getFileInfo(JNIEnv *env, jobject jFS, jobject jPath, hdfsFileInfo *fileInfo)
{
    // JAVA EQUIVALENT:
    //  fs.isDirectory(f)
    //  fs.lastModified() ??
    //  fs.getLength(f)
    //  f.getPath()

    jboolean jIsDir;
    jvalue jVal;

    if (invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                     "exists", JMETHOD1(JPARAM(HADOOP_PATH), "Z"),
                     jPath) != 0) {
        fprintf(stderr, "Call to org.apache.hadoop.fs."
                "FileSystem::exists failed!\n");
        errno = EINTERNAL;
        return -1;
    }

    if (jVal.z == 0) {
      errno = EINTERNAL;
      return -1;
    }

    if (invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                     "isDirectory", "(Lorg/apache/hadoop/fs/Path;)Z",
                     jPath) != 0) {
        fprintf(stderr, "Call to org.apache.hadoop.fs."
                "FileSystem::isDirectory failed!\n");
        errno = EINTERNAL;
        return -1;
    }
    jIsDir = jVal.z;

    /*
    jlong jModTime = 0;
    if (invokeMethod(env, (RetVal*)&jModTime, &jException, INSTANCE, jFS, 
                "org/apache/hadoop/fs/FileSystem", "lastModified", 
                "(Lorg/apache/hadoop/fs/Path;)J", jPath) != 0) {
        fprintf(stderr, 
              "Call to org.apache.hadoop.fs.FileSystem::lastModified failed!\n"
                );
        errno = EINTERNAL;
        return -1;
    }
    */

    jlong jFileLength = 0;
    if (!jIsDir) {
        if (invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS,
                         "getLength", "(Lorg/apache/hadoop/fs/Path;)J",
                         jPath) != 0) {
            fprintf(stderr, "Call to org.apache.hadoop.fs."
                    "FileSystem::getLength failed!\n");
            errno = EINTERNAL;
            return -1;
        }
        jFileLength = jVal.j;
    }

    jstring jPathName;
    if (invokeMethod(env, &jVal, INSTANCE, jPath, HADOOP_PATH,
                     "toString", "()Ljava/lang/String;")) { 
        fprintf(stderr, "Call to org.apache.hadoop.fs."
                "Path::toString failed!\n");
        errno = EINTERNAL;
        return -1;
    }
    jPathName = jVal.l;

    fileInfo->mKind = (jIsDir ? kObjectKindDirectory : kObjectKindFile);
    //fileInfo->mCreationTime = jModTime;
    fileInfo->mSize = jFileLength;

    const char* cPathName = (const char*)
      ((*env)->GetStringUTFChars(env, jPathName, NULL));

    fileInfo->mName = strdup(cPathName);

    (*env)->ReleaseStringUTFChars(env, jPathName, cPathName);

    destroyLocalReference(env, jPathName);

    return 0;
}



hdfsFileInfo* hdfsListDirectory(hdfsFS fs, const char* path, int *numEntries)
{
    // JAVA EQUIVALENT:
    //  Path p(path);
    //  Path []pathList = fs.listPaths(p)
    //  foreach path in pathList 
    //    getFileInfo(path)

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    jobject jFS = (jobject)fs;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return NULL;
    }

    hdfsFileInfo *pathList = 0; 

    jobjectArray jPathList = NULL;
    jvalue jVal;
    if (invokeMethod(env, &jVal, INSTANCE, jFS, HADOOP_FS, "listPaths",
                     JMETHOD1(JPARAM(HADOOP_PATH), JARRPARAM(HADOOP_PATH)),
                     jPath) != 0) {
        fprintf(stderr, "Call to org.apache.hadoop.fs."
                "FileSystem::listPaths failed!\n");
        errno = EINTERNAL;
        destroyLocalReference(env, jPath);
        return NULL;
    }
    jPathList = jVal.l;

    //Figure out no of entries in that directory
    jsize jPathListSize = (*env)->GetArrayLength(env, jPathList);
    *numEntries = jPathListSize;
    if (jPathListSize == 0) {
        errno = 0;
        goto done;
    }

    //Allocate memory
    pathList = calloc(jPathListSize, sizeof(hdfsFileInfo));
    if (pathList == NULL) {
        errno = ENOMEM;
        goto done;
    }

    //Save path information in pathList
    jsize i;
    jobject tmpPath;
    for (i=0; i < jPathListSize; ++i) {
        tmpPath = (*env)->GetObjectArrayElement(env, jPathList, i);
        if (getFileInfo(env, jFS, tmpPath, &pathList[i])) {
            errno = EINTERNAL;
            hdfsFreeFileInfo(pathList, jPathListSize);
            destroyLocalReference(env, tmpPath);
            pathList = NULL;
            goto done;
        }
        destroyLocalReference(env, tmpPath);
    }

    done:

    //Delete unnecessary local references
    destroyLocalReference(env, jPath);
    destroyLocalReference(env, jPathList);

    return pathList;
}



hdfsFileInfo *hdfsGetPathInfo(hdfsFS fs, const char* path)
{
    // JAVA EQUIVALENT:
    //  File f(path);
    //  fs.isDirectory(f)
    //  fs.lastModified() ??
    //  fs.getLength(f)
    //  f.getPath()

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    jobject jFS = (jobject)fs;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return NULL;
    }

    hdfsFileInfo *fileInfo = calloc(1, sizeof(hdfsFileInfo));
    if (getFileInfo(env, jFS, jPath, fileInfo)) {
        hdfsFreeFileInfo(fileInfo, 1);
        fileInfo = NULL;
        goto done;
    }

    done:

    //Delete unnecessary local references
    destroyLocalReference(env, jPath);

    return fileInfo;
}



void hdfsFreeFileInfo(hdfsFileInfo *hdfsFileInfo, int numEntries)
{
    //Free the mName
    int i;
    for (i=0; i < numEntries; ++i) {
        if (hdfsFileInfo[i].mName) {
            free(hdfsFileInfo[i].mName);
        }
    }

    //Free entire block
    free(hdfsFileInfo);
}




/**
 * vim: ts=4: sw=4: et:
 */
