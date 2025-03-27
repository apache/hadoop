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

#include <gmock/gmock.h>
#include <hdfs/hdfs.h>
#include <jni.h>

#ifdef WIN32
#define DECLSPEC
#else
// Windows cribs when this is declared in the function definition,
// However, Linux needs it.
#define DECLSPEC _JNI_IMPORT_OR_EXPORT_
#endif

// hook the jvm runtime function. expect always failure
DECLSPEC jint JNICALL JNI_GetDefaultJavaVMInitArgs(void*) {
    return 1;
}

// hook the jvm runtime function. expect always failure
DECLSPEC jint JNICALL JNI_CreateJavaVM(JavaVM**, void**, void*) {
    return 1;
}

// hook the jvm runtime function. expect always failure
DECLSPEC jint JNICALL JNI_GetCreatedJavaVMs(JavaVM**, jsize, jsize*) {
    return 1;
}

TEST(GetJNITest, TestRepeatedGetJNIFailsButNoCrash) {
    // connect to nothing, should fail but not crash
    EXPECT_EQ(NULL, hdfsConnectNewInstance(NULL, 0));

    // try again, should fail but not crash
    EXPECT_EQ(NULL, hdfsConnectNewInstance(NULL, 0));
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleMock(&argc, argv);
    return RUN_ALL_TESTS();
}
