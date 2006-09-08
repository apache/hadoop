/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

/**
 * hdfsJniEnv: A wrapper struct to be used as 'value'
 * while saving thread -> JNIEnv* mappings
 */
typedef struct 
{
    JNIEnv* env;
} hdfsJniEnv;

/**
 * Helpful macro to convert a pthread_t to a string
 */
#define GET_threadID(threadID, key, keySize) \
    snprintf(key, keySize, "__hdfs_threadID__%u", (unsigned)(threadID)); 
#define threadID_SIZE 32

#define CHECK_jExceptionEPTION_IN_METH_INVOC {\
    jthrowable _jException_;\
    if ((_jException_ = (*env)->jExceptioneptionOccurred(env))) {\
        (*env)->jExceptioneptionDescribe(env);\
        *jException = _jException_;\
        (*env)->jExceptioneptionClear(env);\
        va_end(args);\
        return -1;\
    }\
}

/**
 * getJNIEnv: A helper function to get the JNIEnv* for the given thread.
 * @param: None.
 * @return The JNIEnv* corresponding to the thread.
 */
static inline JNIEnv* getJNIEnv()
{
    char threadID[threadID_SIZE];

    const jsize vmBufLength = 1;
    JavaVM* vmBuf[vmBufLength]; 
    JNIEnv *env;
    jint rv = 0; 
    jint noVMs = 0;

    //Get the threadID and stringize it 
    GET_threadID(pthread_self(), threadID, sizeof(threadID));

    //See if you already have the JNIEnv* cached...
    env = (JNIEnv*)searchEntryFromTable(threadID);
    if (env != NULL) {
        return env; 
    }

    //All right... some serious work required here!
    //1. Initialize the HashTable
    //2. LOCK!
    //3. Check if any JVMs have been created here
    //      Yes: Use it (we should only have 1 VM)
    //      No: Create the JVM
    //4. UNLOCK

    hashTableInit();

    LOCK_HASH_TABLE();

    rv = JNI_GetCreatedJavaVMs(&(vmBuf[0]), vmBufLength, &noVMs);
    if (rv != 0) {
        fprintf(stderr,
                "Call to JNI_GetCreatedJavaVMs failed with error: %d\n", rv);
        exit(1);
    }

    if (noVMs == 0) {
        //Get the environment variables for initializing the JVM
        char *hadoopClassPath = getenv("CLASSPATH");
        if (hadoopClassPath == NULL) {
        		fprintf(stderr, "Please set the environment variable $CLASSPATH!\n");
        		exit(-1);
        } 
        char *hadoopClassPathVMArg = "-Djava.class.path=";
        size_t optHadoopClassPathLen = strlen(hadoopClassPath) + 
        								strlen(hadoopClassPathVMArg) + 1;
        char *optHadoopClassPath = malloc(sizeof(char) * optHadoopClassPathLen);
        snprintf(optHadoopClassPath, optHadoopClassPathLen,
        	"%s%s", hadoopClassPathVMArg, hadoopClassPath);

        //Create the VM
        JavaVMInitArgs vm_args;
        JavaVMOption options[1];
        JavaVM *vm;
        
        // User classes
        options[0].optionString = optHadoopClassPath;
        // Print JNI-related messages      
        //options[2].optionString = "-verbose:jni";

        vm_args.version = JNI_VERSION_1_2;
        vm_args.options = options;
        vm_args.nOptions = 1; 
        vm_args.ignoreUnrecognized = 1;

        rv = JNI_CreateJavaVM(&vm, (void**)&env, &vm_args);
        if (rv != 0) {
            fprintf(stderr, 
                    "Call to JNI_CreateJavaVM failed with error: %d\n");
            exit(1);
        }

        free(optHadoopClassPath);
    } else {
        //Attach this thread to the VM
        JavaVM* vm = vmBuf[0];
        rv = (*vm)->AttachCurrentThread(vm, (void**)&env, 0);
        if (rv != 0) {
            fprintf(stderr, 
                    "Call to AttachCurrentThread failed with error: %d\n");
            exit(1);
        }
    }

    //Save the threadID -> env mapping
    ENTRY e, *ep;
    e.key = threadID;
    e.data = (void*)(env);
    if ((ep = hsearch(e, ENTER)) == NULL) {
        fprintf(stderr, "Call to hsearch(ENTER) failed\n");
        exit(1);
    }

    UNLOCK_HASH_TABLE();

    return env;
}

/**
 * Helper function to create a java.io.File object.
 * @param env: The JNIEnv pointer. 
 * @param path: The file-path for which to construct java.io.File object.
 * @return Returns a jobject on success and NULL on error.
 */
static inline jobject constructNewObjectOfJavaIOFile(JNIEnv *env, const char *path)
{
    //Construct a java.lang.String object
    jstring jPath = (*env)->NewStringUTF(env, path); 

    //Construct the java.io.File object
    jthrowable jException;
    jobject jFile = constructNewObjectOfClass(env, &jException, 
            "java/io/File", "(Ljava/lang/String;)V", jPath);
    if (jFile == NULL) {
        fprintf(stderr, 
                "Can't construct instance of class java.io.File for %s\n",
                path);
        errno = EINTERNAL;
        return NULL;
    }

    //Destroy the java.lang.String object
    (*env)->ReleaseStringUTFChars(env, jPath,
                (*env)->GetStringUTFChars(env, jPath, 0));

    return jFile;
}

/**
 * Helper function to create a org.apache.hadoop.fs.Path object.
 * @param env: The JNIEnv pointer. 
 * @param path: The file-path for which to construct org.apache.hadoop.fs.Path object.
 * @return Returns a jobject on success and NULL on error.
 */
static inline 
jobject constructNewObjectOfPath(JNIEnv *env, const char *path)
{
    //Construct a java.lang.String object
    jstring jPathString = (*env)->NewStringUTF(env, path); 

    //Construct the org.apache.hadoop.fs.Path object
    jthrowable jException;
    jobject jPath = constructNewObjectOfClass(env, &jException, 
            "org/apache/hadoop/fs/Path", "(Ljava/lang/String;)V", jPathString);
    if (jPath == NULL) {
        fprintf(stderr, 
                "Can't construct instance of class org.apache.hadoop.fs.Path for %s\n", 
                path);
        errno = EINTERNAL;
        return NULL;
    }

    //Destroy the java.lang.String object
    (*env)->ReleaseStringUTFChars(env, jPathString,
                (*env)->GetStringUTFChars(env, jPathString, 0));

    return jPath;
}

/**
 * Helper function to destroy a local reference of java.lang.Object
 * @param env: The JNIEnv pointer. 
 * @param jFile: The local reference of java.lang.Object object
 * @return None.
 */
static inline void destroyLocalReference(JNIEnv *env, jobject jObject)
{
  (*env)->DeleteLocalRef(env, jObject);
}

hdfsFS hdfsConnect(const char* host, tPort port)
{
    // JAVA EQUIVALENT:
    //  FileSystem fs = FileSystem.get(new Configuration());
    //  return fs;

    JNIEnv *env = 0;
    jobject jConfiguration;
    jobject jFS;
    jthrowable jException;

    //Get the JNIEnv* corresponding to current thread
    env = getJNIEnv();

    //Create the org.apache.hadoop.conf.Configuration object
    jConfiguration = constructNewObjectOfClass(env, &jException, 
            "org/apache/hadoop/conf/Configuration", "()V");
    if (jConfiguration == NULL) {
        fprintf(stderr,
                "Can't construct instance of class org.apache.hadoop.conf.Configuration\n");
        errno = EINTERNAL;
        return NULL;
    }
 
    //Check what type of FileSystem the caller wants...
    if (host == NULL) {
        //fs = new LocalFileSystem(conf);
        jFS = constructNewObjectOfClass(env, &jException,
                "org/apache/hadoop/fs/LocalFileSystem",
                "(Lorg/apache/hadoop/conf/Configuration;)V", jConfiguration);
        if (jFS == NULL) {
            errno = EINTERNAL;
            goto done;
        }
    } else if (!strcmp(host, "default") && port == 0) {
        //fs = FileSystem::get(conf); 
        if (invokeMethod(env, (RetVal*)&jFS, &jException, STATIC, NULL,
                    "org/apache/hadoop/fs/FileSystem", "get", 
                    "(Lorg/apache/hadoop/conf/Configuration;)Lorg/apache/hadoop/fs/FileSystem;", 
                    jConfiguration) != 0) {
            fprintf(stderr, 
                    "Call to org.apache.hadoop.fs.FileSystem::get failed!\n");
            errno = EINTERNAL;
            goto done;
        }
    } else {
        //fs = new DistributedFileSystem(new InetSocketAddress(host, port), conf)
        jstring jHostName = (*env)->NewStringUTF(env, host);
    
        jobject jNameNode = constructNewObjectOfClass(env, &jException,
                "java/net/InetSocketAddress", "(Ljava/lang/String;I)V", 
                jHostName, port);
        (*env)->ReleaseStringUTFChars(env, jHostName,
                            (*env)->GetStringUTFChars(env, jHostName, NULL));
        if (jNameNode == NULL) {
            errno = EINTERNAL;
            goto done;
        }
    
        jFS = constructNewObjectOfClass(env, &jException,
                "org/apache/hadoop/dfs/DistributedFileSystem",
                "(Ljava/net/InetSocketAddress;Lorg/apache/hadoop/conf/Configuration;)V", 
                jNameNode, jConfiguration);
        destroyLocalReference(env, jNameNode);
        if (jFS == NULL) {
            errno = EINTERNAL;
            goto done;
        }
    }

    done:
    
    //Release unnecessary local references
    destroyLocalReference(env, jConfiguration);

    return jFS;
}

int hdfsDisconnect(hdfsFS fs)
{
    // JAVA EQUIVALENT:
    //  fs.close()

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    //Parameters
    jobject jFS = (jobject)fs;

    //jException reference
    jthrowable jException;

    //Sanity check
    if (fs == NULL) {
        errno = EBADF;
        return -1;
    }

    if (invokeMethod(env, NULL, &jException, INSTANCE, jFS, 
                "org/apache/hadoop/fs/FileSystem",
                "close", "()V") != 0) {
        fprintf(stderr, "Call to FileSystem::close failed!\n"); 
        errno = EINTERNAL;
        return -1;
    }

    //Release unnecessary local references
    destroyLocalReference(env, jFS);

    return 0;
}

hdfsFile hdfsOpenFile(hdfsFS fs, const char* path, int flags, 
        int bufferSize, short replication, tSize blockSize)
{
    // JAVA EQUIVALENT:
    //  File f = new File(path);
    //  FSData{Input|Output}Stream f{is|os} = fs.create(f);
    //  return f{is|os};

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    jobject jFS = (jobject)fs;
    jthrowable jException;

    //The hadoop java api/signature
    const char* method = (flags == O_RDONLY) ? "open" : "create";
    const char* signature = (flags == O_RDONLY) ? 
        "(Lorg/apache/hadoop/fs/Path;I)Lorg/apache/hadoop/fs/FSDataInputStream;" : 
        "(Lorg/apache/hadoop/fs/Path;ZISJ)Lorg/apache/hadoop/fs/FSDataOutputStream;";

    //Return value
    hdfsFile file = NULL;

    //Create an object of org.apache.hadoop.fs.Path 
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return NULL; 
    }

    //Create the org.apache.hadoop.conf.Configuration object
    //and get the configured values if need be
    jobject jConfiguration = constructNewObjectOfClass(env, &jException, 
            "org/apache/hadoop/conf/Configuration", "()V");
    if (jConfiguration == NULL) {
        fprintf(stderr,
                "Can't construct instance of class org.apache.hadoop.conf.Configuration\n");
        errno = EINTERNAL;
        return NULL;
    }
    jint jBufferSize = bufferSize;
    jshort jReplication = replication;
    jlong jBlockSize = blockSize;
    jstring jStrBufferSize = (*env)->NewStringUTF(env, "io.file.buffer.size"); 
    jstring jStrReplication = (*env)->NewStringUTF(env, "dfs.replication");
    jstring jStrBlockSize = (*env)->NewStringUTF(env, "dfs.block.size"); 

    //bufferSize
    if(!bufferSize) {
        if (invokeMethod(env, (RetVal*)&jBufferSize, &jException, INSTANCE, jConfiguration, 
                    "org/apache/hadoop/conf/Configuration", "getInt",
                    "(Ljava/lang/String;I)I", jStrBufferSize, 4096)) {
            fprintf(stderr,
                    "Call to org.apache.hadoop.conf.Configuration::getInt failed!\n");
            errno = EINTERNAL;
            goto done;
        }
    }

    if(flags == O_WRONLY) {
        //replication
        jint jTmpReplication;
        if(!replication) {
            if (invokeMethod(env, (RetVal*)&jTmpReplication, &jException, INSTANCE, jConfiguration, 
                        "org/apache/hadoop/conf/Configuration", "getInt",
                        "(Ljava/lang/String;I)I", jStrReplication, 1)) {
                fprintf(stderr,
                        "Call to org.apache.hadoop.conf.Configuration::getInt failed!\n");
                errno = EINTERNAL;
                goto done;
            }
            jReplication = jTmpReplication;
        }
        
        //blockSize
        if(!blockSize) {
            if (invokeMethod(env, (RetVal*)&jBlockSize, &jException, INSTANCE, jConfiguration, 
                        "org/apache/hadoop/conf/Configuration", "getLong",
                        "(Ljava/lang/String;J)J", jStrBlockSize, 67108864)) {
                fprintf(stderr,
                        "Call to org.apache.hadoop.fs.FileSystem::%s(%s) failed!\n", 
                        method, signature);
                errno = EINTERNAL;
                goto done;
            }
        }
    }
 
    //Create and return either the FSDataInputStream or FSDataOutputStream references 
    jobject jStream;
    if(flags == O_RDONLY) {
        if (invokeMethod(env, (RetVal*)&jStream, &jException, INSTANCE, jFS, 
                    "org/apache/hadoop/fs/FileSystem", 
                    method, signature, jPath, jBufferSize)) {
            fprintf(stderr,
                    "Call to org.apache.hadoop.fs.FileSystem::%s(%s) failed!\n", 
                    method, signature);
            errno = EINTERNAL;
            goto done;
        }
    } else {
        jboolean jOverWrite = 1;
        if (invokeMethod(env, (RetVal*)&jStream, &jException, INSTANCE, jFS, 
                    "org/apache/hadoop/fs/FileSystem", 
                    method, signature, jPath, jOverWrite, jBufferSize, jReplication, jBlockSize)) {
            fprintf(stderr,
                    "Call to org.apache.hadoop.fs.FileSystem::%s(%s) failed!\n", 
                    method, signature);
            errno = EINTERNAL;
            goto done;
        }
    }
  
    file = malloc(sizeof(struct hdfsFile_internal));
    if (!file) {
        errno = ENOMEM;
        return NULL;
    }
    file->file = (void*)jStream;
    file->type = ((flags & O_RDONLY) ? INPUT : OUTPUT);

    done:

    //Delete unnecessary local references
    (*env)->ReleaseStringUTFChars(env, jStrBufferSize,
                (*env)->GetStringUTFChars(env, jStrBufferSize, 0));
    (*env)->ReleaseStringUTFChars(env, jStrReplication,
                (*env)->GetStringUTFChars(env, jStrReplication, 0));
    (*env)->ReleaseStringUTFChars(env, jStrBlockSize,
                (*env)->GetStringUTFChars(env, jStrBlockSize, 0));
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
    jobject jFS = (jobject)fs;
    jobject jStream = (jobject)(file ? file->file : NULL);

    //jException reference
    jthrowable jException;

    //Sanity check
    if (!file || file->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }

    //The interface whose 'close' method to be called
    const char* interface = (file->type == INPUT) ? 
        "org/apache/hadoop/fs/FSDataInputStream" : 
        "org/apache/hadoop/fs/FSDataOutputStream";
  
    if (invokeMethod(env, NULL, &jException, INSTANCE, jStream, interface,
                "close", "()V") != 0) {
        fprintf(stderr, "Call to %s::close failed!\n", interface); 
        errno = EINTERNAL;
        return -1;
    }

    //De-allocate memory
    free(file);

    return 0;
}

tSize hdfsRead(hdfsFS fs, hdfsFile f, void* buffer, tSize length)
{
    // JAVA EQUIVALENT:
    //  byte [] bR = new byte[length];
    //  fis.read(bR);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    //Parameters
    jobject jFS = (jobject)fs;
    jobject jInputStream = (jobject)(f ? f->file : NULL);

    jthrowable jException;
    jbyteArray jbRarray;
    jint noReadBytes = 0;

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
    if (invokeMethod(env, (RetVal*)&noReadBytes, &jException, INSTANCE, 
                jInputStream, "org/apache/hadoop/fs/FSDataInputStream", 
                "read", "([B)I", jbRarray) != 0) {
        fprintf(stderr, 
            "Call to org.apache.hadoop.fs.FSDataInputStream::read failed!\n");
        errno = EINTERNAL;
        noReadBytes = -1;
    } else {
        if(noReadBytes > 0) {
            (*env)->GetByteArrayRegion(env, jbRarray, 0, noReadBytes, buffer);
        }
        //This is a valid case: there aren't any bytes left to read!
        errno = 0;
    }
    (*env)->ReleaseByteArrayElements(env, jbRarray, 
                (*env)->GetByteArrayElements(env, jbRarray, 0), JNI_ABORT);

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
    jobject jFS = (jobject)fs;
    jobject jOutputStream = (jobject)(f ? f->file : 0);

    jthrowable jException;
    jbyteArray jbWarray;
    jint noWrittenBytes = 0;

    //Sanity check
    if (!f || f->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }

    //Error checking... make sure that this file is 'writable'
    if (f->type != OUTPUT) {
        fprintf(stderr, "Cannot write into a non-OutputStream object!\n");
        errno = EINVAL;
        return -1;
    }

    //Write the requisite bytes into the file
    jbWarray = (*env)->NewByteArray(env, length);
    (*env)->SetByteArrayRegion(env, jbWarray, 0, length, buffer);
    if (invokeMethod(env, NULL, &jException, INSTANCE, jOutputStream,
                "org/apache/hadoop/fs/FSDataOutputStream", "write", 
                "([B)V", jbWarray)) {
        fprintf(stderr, 
            "Call to org.apache.hadoop.fs.FSDataOutputStream::write failed!\n"
            );
        errno = EINTERNAL;
        noWrittenBytes = -1;
    } 
    (*env)->ReleaseByteArrayElements(env, jbWarray, 
                (*env)->GetByteArrayElements(env, jbWarray, 0), JNI_ABORT);

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
    jobject jFS = (jobject)fs;
    jobject jInputStream = (jobject)(f ? f->file : 0);

    jthrowable jException;

    //Sanity check
    if (!f || f->type != INPUT) {
        errno = EBADF;
        return -1;
    }

    if (invokeMethod(env, NULL, &jException, INSTANCE, jInputStream, 
                "org/apache/hadoop/fs/FSDataInputStream", "seek", 
                "(J)V", desiredPos) != 0) {
        fprintf(stderr, 
            "Call to org.apache.hadoop.fs.FSDataInputStream::seek failed!\n");
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
    jobject jFS = (jobject)fs;
    jobject jStream = (jobject)(f ? f->file : 0);

    jthrowable jException;

    //Sanity check
    if (!f || f->type == UNINITIALIZED) {
        errno = EBADF;
        return -1;
    }

    const char* interface = (f->type == INPUT) ? 
        "org/apache/hadoop/fs/FSDataInputStream" : 
        "org/apache/hadoop/fs/FSDataOutputStream";

    jlong currentPos  = -1;
    if (invokeMethod(env,(RetVal*)&currentPos, &jException, INSTANCE, 
                jStream, interface, "getPos", "()J") != 0) {
        fprintf(stderr, "Call to org.apache.hadoop.fs.FSDataInputStream::getPos failed!\n");
        errno = EINTERNAL;
        return -1;
    }

    return (tOffset)currentPos;
}

int hdfsFlush(hdfsFS fs, hdfsFile f) 
{
    // JAVA EQUIVALENT
    //  fos.flush();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    //Parameters
    jobject jFS = (jobject)fs;
    jobject jOutputStream = (jobject)(f ? f->file : 0);

    jthrowable jException;

    //Sanity check
    if (!f || f->type != OUTPUT) {
        errno = EBADF;
        return -1;
    }

    if (invokeMethod(env, NULL, &jException, INSTANCE, jOutputStream, 
                "org/apache/hadoop/fs/FSDataOutputStream", "flush", 
                "()V") != 0) {
        fprintf(stderr, 
                "Call to org.apache.hadoop.fs.FSDataInputStream::flush failed!\n"
                );
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
    jobject jFS = (jobject)fs;
    jobject jInputStream = (jobject)(f ? f->file : 0);

    jthrowable jException;

    //Sanity check
    if (!f || f->type != INPUT) {
        errno = EBADF;
        return -1;
    }

    jint available = -1;
    if (invokeMethod(env, (RetVal*)&available, &jException, INSTANCE, jInputStream, 
                "org/apache/hadoop/fs/FSDataInputStream", "available", 
                "()I") != 0) {
        fprintf(stderr, 
            "Call to org.apache.hadoop.fs.FSDataInputStream::available failed!\n"
            );
        errno = EINTERNAL;
        return -1;
    }

    return available;
}

int hdfsCopy(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst)
{
    //JAVA EQUIVALENT
    //  FileUtil::copy(srcFS, srcPath, dstFS, dstPath, deleteSource = false, conf)

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    //Parameters
    jobject jSrcFS = (jobject)srcFS;
    jobject jDstFS = (jobject)dstFS;
    jobject jSrcPath = constructNewObjectOfPath(env, src);
    jobject jDstPath = constructNewObjectOfPath(env, dst);
    if (jSrcPath == NULL || jDstPath == NULL) {
        return -1;
    }
    jthrowable jException;
    int retval = 0;

    //Create the org.apache.hadoop.conf.Configuration object
    jobject jConfiguration = constructNewObjectOfClass(env, &jException, 
            "org/apache/hadoop/conf/Configuration", "()V");
    if (jConfiguration == NULL) {
        fprintf(stderr, 
                "Can't construct instance of class org.apache.hadoop.conf.Configuration\n"
                );
        errno = EINTERNAL;
        return -1;
    }

    //FileUtil::copy
    jboolean deleteSource = 0; //Only copy
    jboolean jRetVal = 0;
    if (invokeMethod(env, (RetVal*)&jRetVal, &jException, STATIC, 
                NULL, "org/apache/hadoop/fs/FileUtil", "copy",
                "(Lorg/apache/hadoop/fs/FileSystem;Lorg/apache/hadoop/fs/Path;Lorg/apache/hadoop/fs/FileSystem;Lorg/apache/hadoop/fs/Path;ZLorg/apache/hadoop/conf/Configuration;)Z",
                jSrcFS, jSrcPath, jDstFS, jDstPath, deleteSource, 
                jConfiguration) != 0) {
        fprintf(stderr, 
          "Call to org.apache.hadoop.fs.FileUtil::copy failed!\n");
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
    //  FileUtil::copy(srcFS, srcPath, dstFS, dstPath, deleteSource = true, conf)

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    //Parameters
    jobject jSrcFS = (jobject)srcFS;
    jobject jDstFS = (jobject)dstFS;
    jobject jSrcPath = constructNewObjectOfPath(env, src);
    jobject jDstPath = constructNewObjectOfPath(env, dst);
    if (jSrcPath == NULL || jDstPath == NULL) {
        return -1;
    }
    jthrowable jException;
    int retval = 0;

    //Create the org.apache.hadoop.conf.Configuration object
    jobject jConfiguration = constructNewObjectOfClass(env, &jException, 
            "org/apache/hadoop/conf/Configuration", "()V");
    if (jConfiguration == NULL) {
        fprintf(stderr, 
                "Can't construct instance of class org.apache.hadoop.conf.Configuration\n"
                );
        errno = EINTERNAL;
        return -1;
    }

    //FileUtil::copy
    jboolean deleteSource = 1; //Delete src after copy
    jboolean jRetVal = 0;
    if (invokeMethod(env, (RetVal*)&jRetVal, &jException, STATIC, 
                NULL, "org/apache/hadoop/fs/FileUtil", "copy",
                "(Lorg/apache/hadoop/fs/FileSystem;Lorg/apache/hadoop/fs/Path;Lorg/apache/hadoop/fs/FileSystem;Lorg/apache/hadoop/fs/Path;ZLorg/apache/hadoop/conf/Configuration;)Z",
                jSrcFS, jSrcPath, jDstFS, jDstPath, deleteSource, 
                jConfiguration) != 0) {
        fprintf(stderr, 
          "Call to org.apache.hadoop.fs.FileUtil::copy(move) failed!\n");
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
    jthrowable jException;

    //Create an object of java.io.File
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return -1;
    }

    //Delete the file
    jboolean retval = 1;
    if (invokeMethod(env, (RetVal*)&retval, &jException, INSTANCE, jFS, 
                "org/apache/hadoop/fs/FileSystem", "delete", 
                "(Lorg/apache/hadoop/fs/Path;)Z", jPath)) {
        fprintf(stderr, 
                "Call to org.apache.hadoop.fs.FileSystem::delete failed!\n");
        errno = EINTERNAL;
        return -1;
    }

    //Delete unnecessary local references
    destroyLocalReference(env, jPath);

    return (retval) ? 0 : -1;
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
    jthrowable jException;

    //Create objects of org.apache.hadoop.fs.Path
    jobject jOldPath = constructNewObjectOfPath(env, oldPath);
    jobject jNewPath = constructNewObjectOfPath(env, newPath);
    if (jOldPath == NULL || jNewPath == NULL) {
        return -1;
    }

    //Rename the file
    jboolean retval = 1;
    if (invokeMethod(env, (RetVal*)&retval, &jException, INSTANCE, jFS, 
                "org/apache/hadoop/fs/FileSystem", "rename", 
                "(Lorg/apache/hadoop/fs/Path;Lorg/apache/hadoop/fs/Path;)Z", 
                jOldPath, jNewPath)) {
        fprintf(stderr, 
                "Call to org.apache.hadoop.fs.FileSystem::rename failed!\n");
        errno = EINTERNAL;
        return -1;
    }

    //Delete unnecessary local references
    destroyLocalReference(env, jOldPath);
    destroyLocalReference(env, jNewPath);

    return (retval) ? 0 : -1;
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

    jthrowable jException;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return -1;
    }

    //Lock the file
    int retval = 0;
    if (invokeMethod(env, NULL, &jException, INSTANCE, jFS, 
                "org/apache/hadoop/fs/FileSystem", "lock", 
                "(Lorg/apache/hadoop/fs/Path;Z)V", jPath, jb_shared)) {
        fprintf(stderr, 
                "Call to org.apache.hadoop.fs.FileSystem::lock failed!\n");
        errno = EINTERNAL;
        retval = -1;
    }

    done:

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
    jthrowable jException;

    //Create an object of java.io.File
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return -1;
    }

    //Release the lock on the file
    int retval = 0;
    if (invokeMethod(env, NULL, &jException, INSTANCE, jFS, 
                "org/apache/hadoop/fs/FileSystem", "release", 
                "(Lorg/apache/hadoop/fs/Path;)V", jPath)) {
        fprintf(stderr, 
                "Call to org.apache.hadoop.fs.FileSystem::release failed!\n");
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
    jthrowable jException;

    //FileSystem::getWorkingDirectory()
    if (invokeMethod(env, (RetVal*)&jPath, &jException, INSTANCE, jFS, 
                "org/apache/hadoop/fs/FileSystem", "getWorkingDirectory", 
                "()Lorg/apache/hadoop/fs/Path;") || jPath == NULL) {
        fprintf(stderr, "Call to FileSystem::getWorkingDirectory failed!\n");
        errno = EINTERNAL;
        return NULL;
    }

    //Path::toString()
    jstring jPathString;
    if (invokeMethod(env, (RetVal*)&jPathString, &jException, INSTANCE, jPath, 
                "org/apache/hadoop/fs/Path", "toString", "()Ljava/lang/String;")) { 
        fprintf(stderr, "Call to Path::toString failed!\n");
        errno = EINTERNAL;
        destroyLocalReference(env, jPath);
        return NULL;
    }

    //Copy to user-provided buffer
    strncpy(buffer, (char*)(*env)->GetStringUTFChars(env, jPathString, NULL), 
            bufferSize);

    //Delete unnecessary local references
    (*env)->ReleaseStringUTFChars(env, jPathString, 
                                (*env)->GetStringUTFChars(env, jPathString, NULL));
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
    jthrowable jException;

    int retval = 0;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return -1;
    }

    //FileSystem::setWorkingDirectory()
    if (invokeMethod(env, NULL, &jException, INSTANCE, jFS, 
                "org/apache/hadoop/fs/FileSystem", "setWorkingDirectory", 
                "(Lorg/apache/hadoop/fs/Path;)V", jPath) || jPath == NULL) {
        fprintf(stderr, "Call to FileSystem::setWorkingDirectory failed!\n");
        errno = EINTERNAL;
        retval = -1;
    }

    done:
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
    jthrowable jException;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return -1;
    }

    //Create the directory
    jboolean jRetVal = 0;
    if (invokeMethod(env, (RetVal*)&jRetVal, &jException, INSTANCE, jFS, 
                "org/apache/hadoop/fs/FileSystem", "mkdirs", 
                "(Lorg/apache/hadoop/fs/Path;)Z", jPath)) {
        fprintf(stderr, 
                "Call to org.apache.hadoop.fs.FileSystem::mkdirs failed!\n");
        errno = EINTERNAL;
        goto done;
    }

    done:

    //Delete unnecessary local references
    destroyLocalReference(env, jPath);

    return (jRetVal) ? 0 : -1;
}

char*** hdfsGetHosts(hdfsFS fs, const char* path, tOffset start, tOffset length)
{
    // JAVA EQUIVALENT:
    //  fs.getFileCacheHints(new Path(path), start, length);

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    jobject jFS = (jobject)fs;
    jthrowable jException;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return NULL;
    }

    //org.apache.hadoop.fs.FileSystem::getFileCacheHints
    char*** blockHosts = NULL;
    jobjectArray jFileCacheHints;
    if (invokeMethod(env, (RetVal*)&jFileCacheHints, &jException, INSTANCE, 
                jFS, "org/apache/hadoop/fs/FileSystem", "getFileCacheHints", 
                "(Lorg/apache/hadoop/fs/Path;JJ)[[Ljava/lang/String;", jPath, 
                start, length)) {
        fprintf(stderr, 
                "Call to org.apache.hadoop.fs.FileSystem::getFileCacheHints failed!\n"
               );
        errno = EINTERNAL;
        goto done;
    }

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
    for(i=0; i < jNumFileBlocks; ++i) {
        jobjectArray jFileBlockHosts = (*env)->GetObjectArrayElement(env, 
                                                        jFileCacheHints, i);

        //Figure out no of entries in jFileCacheHints 
        //Allocate memory and add NULL at the end
        jsize jNumBlockHosts = (*env)->GetArrayLength(env, jFileBlockHosts);
        blockHosts[i] = malloc(sizeof(char*) * (jNumBlockHosts+1));
        if (blockHosts[i] == NULL) {
            int x = 0;
            for(x=0; x < i; ++x) {
                free(blockHosts[x]);
            }
            free(blockHosts);
            errno = ENOMEM;
            goto done;
        }
        blockHosts[i][jNumBlockHosts] = NULL;

        //Now parse each hostname
        int j = 0;
        for(j=0; j < jNumBlockHosts; ++j) {
            jstring jHost = (*env)->GetObjectArrayElement(env, 
                    jFileBlockHosts, j);
            blockHosts[i][j] = strdup((char*)(*env)->GetStringUTFChars(env, 
                                                jHost, NULL));
            (*env)->ReleaseStringUTFChars(env, jHost, 
                                (*env)->GetStringUTFChars(env, jHost, NULL));
        }
    }
  
    done:

    //Delete unnecessary local references
    destroyLocalReference(env, jPath);

    return blockHosts;
}

tOffset hdfsGetDefaultBlockSize(hdfsFS fs)
{
    // JAVA EQUIVALENT:
    //  fs.getDefaultBlockSize();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    jobject jFS = (jobject)fs;
    jthrowable jException;

    //FileSystem::getDefaultBlockSize()
    tOffset blockSize = -1;
    if (invokeMethod(env, (RetVal*)&blockSize, &jException, INSTANCE, jFS, 
                "org/apache/hadoop/fs/FileSystem", "getDefaultBlockSize", 
                "()J") != 0) {
        fprintf(stderr, 
                "Call to org.apache.hadoop.fs.FileSystem::getDefaultBlockSize failed!\n"
                );
        errno = EINTERNAL;
        return -1;
    }

    return blockSize;
}

tOffset hdfsGetCapacity(hdfsFS fs)
{
    // JAVA EQUIVALENT:
    //  fs.getRawCapacity();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    jobject jFS = (jobject)fs;
    jthrowable jException;

    if (!((*env)->IsInstanceOf(env, jFS, 
                    globalClassReference("org/apache/hadoop/dfs/DistributedFileSystem", 
                        env)))) {
        fprintf(stderr, 
                "hdfsGetCapacity works only on a DistributedFileSystem!\n");
        return -1;
    }

    //FileSystem::getRawCapacity()
    tOffset rawCapacity = -1;
    if (invokeMethod(env, (RetVal*)&rawCapacity, &jException, INSTANCE, jFS, 
                "org/apache/hadoop/dfs/DistributedFileSystem", 
                "getRawCapacity", "()J") != 0) {
        fprintf(stderr, 
            "Call to org.apache.hadoop.fs.FileSystem::getRawCapacity failed!\n"
            );
        errno = EINTERNAL;
        return -1;
    }

    return rawCapacity;
}
  
tOffset hdfsGetUsed(hdfsFS fs)
{
    // JAVA EQUIVALENT:
    //  fs.getRawUsed();

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    jobject jFS = (jobject)fs;
    jthrowable jException;

    if (!((*env)->IsInstanceOf(env, jFS, 
                    globalClassReference("org/apache/hadoop/dfs/DistributedFileSystem", 
                        env)))) {
        fprintf(stderr, 
                "hdfsGetUsed works only on a DistributedFileSystem!\n");
        return -1;
    }

    //FileSystem::getRawUsed()
    tOffset rawUsed = -1;
    if (invokeMethod(env, (RetVal*)&rawUsed, &jException, INSTANCE, jFS, 
                "org/apache/hadoop/dfs/DistributedFileSystem", "getRawUsed", 
                "()J") != 0) {
        fprintf(stderr, 
            "Call to org.apache.hadoop.fs.FileSystem::getRawUsed failed!\n");
        errno = EINTERNAL;
        return -1;
    }

    return rawUsed;
}
 
static int getFileInfo(JNIEnv *env, jobject jFS, jobject jPath, hdfsFileInfo *fileInfo)
{
    // JAVA EQUIVALENT:
    //  fs.isDirectory(f)
    //  fs.lastModified() ??
    //  fs.getLength(f)
    //  f.getPath()

    jthrowable jException;

    jboolean jIsDir;
    if (invokeMethod(env, (RetVal*)&jIsDir, &jException, INSTANCE, jFS, 
                "org/apache/hadoop/fs/FileSystem", "isDirectory", 
                "(Lorg/apache/hadoop/fs/Path;)Z", jPath) != 0) {
        fprintf(stderr, 
                "Call to org.apache.hadoop.fs.FileSystem::isDirectory failed!\n"
                );
        errno = EINTERNAL;
        return -1;
    }

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
        if (invokeMethod(env, (RetVal*)&jFileLength, &jException, INSTANCE, 
                    jFS, "org/apache/hadoop/fs/FileSystem", "getLength", 
                    "(Lorg/apache/hadoop/fs/Path;)J", jPath) != 0) {
            fprintf(stderr, 
                    "Call to org.apache.hadoop.fs.FileSystem::getLength failed!\n"
                    );
            errno = EINTERNAL;
            return -1;
        }
    }

    jstring jPathName;
    if (invokeMethod(env, (RetVal*)&jPathName, &jException, INSTANCE, jPath, 
                "org/apache/hadoop/fs/Path", "toString", "()Ljava/lang/String;")) { 
        fprintf(stderr, "Call to org.apache.hadoop.fs.Path::toString failed!\n");
        errno = EINTERNAL;
        return -1;
    }

    fileInfo->mKind = (jIsDir ? kObjectKindDirectory : kObjectKindFile);
    //fileInfo->mCreationTime = jModTime;
    fileInfo->mSize = jFileLength;
    fileInfo->mName = strdup((char*)(*env)->GetStringUTFChars(env, 
                jPathName, NULL));

    (*env)->ReleaseStringUTFChars(env, jPathName,
                               (*env)->GetStringUTFChars(env, jPathName, NULL));

    return 0;
}

hdfsFileInfo *hdfsListDirectory(hdfsFS fs, const char* path, int *numEntries)
{
    // JAVA EQUIVALENT:
    //  Path p(path);
    //  Path []pathList = fs.listPaths(p)
    //  foreach path in pathList 
    //    getFileInfo(path)

    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    jobject jFS = (jobject)fs;
    jthrowable jException;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return NULL;
    }

    hdfsFileInfo *pathList = 0; 

    jobjectArray jPathList;
    if (invokeMethod(env, (RetVal*)&jPathList, &jException, INSTANCE, jFS, 
                "org/apache/hadoop/fs/FileSystem", "listPaths", 
                "(Lorg/apache/hadoop/fs/Path;)[Lorg/apache/hadoop/fs/Path;", jPath) != 0) {
        fprintf(stderr, 
                "Call to org.apache.hadoop.fs.FileSystem::listPaths failed!\n"
                );
        errno = EINTERNAL;
        goto done;
    }

    //Figure out no of entries in that directory
    jsize jPathListSize = (*env)->GetArrayLength(env, jPathList);
    *numEntries = jPathListSize;
    if (jPathListSize == 0) {
        errno = 0;
        goto done;
    }

    //Allocate memory
    pathList = malloc(sizeof(hdfsFileInfo) * jPathListSize);
    if (pathList == NULL) {
        errno = ENOMEM;
        goto done;
    }

    //Save path information in pathList
    jsize i;
    for(i=0; i < jPathListSize; ++i) {
        if (getFileInfo(env, jFS, (*env)->GetObjectArrayElement(env, 
                        jPathList, i), &pathList[i])) {
            errno = EINTERNAL;
            free(pathList);
            goto done;
        }
    }

    done:

    //Delete unnecessary local references
    destroyLocalReference(env, jPath);

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
    jthrowable jException;

    //Create an object of org.apache.hadoop.fs.Path
    jobject jPath = constructNewObjectOfPath(env, path);
    if (jPath == NULL) {
        return NULL;
    }

    hdfsFileInfo *fileInfo = malloc(sizeof(hdfsFileInfo));
    bzero(fileInfo, sizeof(hdfsFileInfo));
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

jobject hdfsConvertToGlobalRef(jobject localRef)
{
    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    //Create the global reference
    jobject globalRef = (*env)->NewGlobalRef(env, localRef);
    if(globalRef == NULL) {
        (*env)->ExceptionDescribe(env);
        return NULL; 
    }

    //Destroy the local reference
    (*env)->DeleteLocalRef(env, globalRef);

    return globalRef;
}

void hdfsDeleteGlobalRef(jobject globalRef)
{
    //Get the JNIEnv* corresponding to current thread
    JNIEnv* env = getJNIEnv();

    //Destroy the global reference
    (*env)->DeleteGlobalRef(env, globalRef);
}

/**
 * vim: ts=4: sw=4: et:
 */
