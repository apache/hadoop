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

#include "hdfspp_mini_dfs.h"
#include "hdfspp/hdfs_ext.h"

#include <cstring>
#include <chrono>
#include <exception>

namespace hdfs {

class HdfsExtTest: public ::testing::Test {
public:
  MiniCluster cluster;
};

// Make sure we can set up a mini-cluster and connect to it
TEST_F(HdfsExtTest, TestGetBlockLocations) {
  HdfsHandle connection = cluster.connect_c();
  EXPECT_NE(nullptr, connection.handle());

  hdfsBlockLocations * blocks = nullptr;

  // Free a null pointer
  int result = hdfsFreeBlockLocations(blocks);
  EXPECT_EQ(0, result);

  // Test non-extant files
  EXPECT_EQ(-1, hdfsGetBlockLocations(connection, "non_extant_file", &blocks));  // Should be an error
  EXPECT_EQ((int) std::errc::no_such_file_or_directory, errno);

  // Test an extant file
  std::string filename = connection.newFile(1024);
  result = hdfsGetBlockLocations(connection, filename.c_str(), &blocks);
  EXPECT_EQ(0, result);

  EXPECT_EQ(1024, blocks->fileLength);
  EXPECT_EQ(1, blocks->num_blocks);
  EXPECT_EQ(0, blocks->isUnderConstruction);
  EXPECT_NE(0, blocks->isLastBlockComplete);
  EXPECT_EQ(1024, blocks->blocks->num_bytes);
  EXPECT_EQ(0, blocks->blocks->start_offset);
  EXPECT_EQ(1, blocks->blocks->num_locations);
  EXPECT_NE(nullptr, blocks->blocks->locations->hostname);
  EXPECT_NE(nullptr, blocks->blocks->locations->ip_address);
  EXPECT_NE(nullptr, blocks->blocks->locations->network_location);
  EXPECT_NE(0, blocks->blocks->locations->xfer_port);

  result = hdfsFreeBlockLocations(blocks);
  EXPECT_EQ(0, result);

}


// Writing a file to the filesystem and checking the used space
TEST_F(HdfsExtTest, TestGetUsed) {
  using namespace std::chrono;

  HdfsHandle connection = cluster.connect_c();
  hdfsFS fs = connection.handle();
  EXPECT_NE(nullptr, fs);

  // File system's used space before writing
  tOffset used_before_write;
  EXPECT_GE(used_before_write = hdfsGetUsed(fs), 0);

  // Write to a file
  tOffset fileSize = 1024;
  std::string filename = connection.newFile(fileSize);

  //Need to run hdfsGetUsed() in a loop until the refreshInterval
  //is passed on the filesystem and the used space is updated
  //Time-out is 3 minutes
  tOffset used_after_write;
  tOffset difference;
  minutes beginTime = duration_cast<minutes>(
      system_clock::now().time_since_epoch());
  minutes currentTime;
  do{
    EXPECT_GE(used_after_write = hdfsGetUsed(fs), 0);
    difference = used_after_write - used_before_write;
    currentTime = duration_cast<minutes>(
          system_clock::now().time_since_epoch());
  } while (difference == 0 && currentTime.count() - beginTime.count() < 3);

  //There should be at least fileSize bytes added to the used space
  EXPECT_GT(difference, fileSize);
  //There could be additional metadata added to the used space,
  //but no more than double the fileSize
  EXPECT_LT(difference, fileSize * 2);

}


//Testing allow, disallow, create, and delete snapshot
TEST_F(HdfsExtTest, TestSnapshotOperations) {
  HdfsHandle connection = cluster.connect_c();
  hdfsFS fs = connection.handle();
  EXPECT_NE(nullptr, fs);

  //argument 'path' is NULL
  EXPECT_EQ(-1, hdfsAllowSnapshot(fs, nullptr));
  EXPECT_EQ((int) std::errc::invalid_argument, errno);
  EXPECT_EQ(-1, hdfsCreateSnapshot(fs, nullptr, "Bad"));
  EXPECT_EQ((int) std::errc::invalid_argument, errno);
  EXPECT_EQ(-1, hdfsDeleteSnapshot(fs, nullptr, "Bad"));
  EXPECT_EQ((int) std::errc::invalid_argument, errno);
  EXPECT_EQ(-1, hdfsRenameSnapshot(fs, nullptr, "Bad", "Bad"));
  EXPECT_EQ((int) std::errc::invalid_argument, errno);
  EXPECT_EQ(-1, hdfsDisallowSnapshot(fs, nullptr));
  EXPECT_EQ((int) std::errc::invalid_argument, errno);

  //argument 'name' is NULL for deletion
  EXPECT_EQ(-1, hdfsDeleteSnapshot(fs, "/dir/", nullptr));
  EXPECT_EQ((int) std::errc::invalid_argument, errno);

  //Path not found
  std::string path = "/wrong/dir/";
  EXPECT_EQ(-1, hdfsAllowSnapshot(fs, path.c_str()));
  EXPECT_EQ((int) std::errc::no_such_file_or_directory, errno);
  EXPECT_EQ(-1, hdfsCreateSnapshot(fs, path.c_str(), "Bad"));
  EXPECT_EQ((int) std::errc::no_such_file_or_directory, errno);
  EXPECT_EQ(-1, hdfsDeleteSnapshot(fs, path.c_str(), "Bad"));
  EXPECT_EQ((int) std::errc::no_such_file_or_directory, errno);
  EXPECT_EQ(-1, hdfsRenameSnapshot(fs, path.c_str(), "Bad", "Bad"));
  EXPECT_EQ((int) std::errc::no_such_file_or_directory, errno);
  EXPECT_EQ(-1, hdfsDisallowSnapshot(fs, path.c_str()));
  EXPECT_EQ((int) std::errc::no_such_file_or_directory, errno);

  //Not a directory
  path = connection.newFile(1024); //1024 byte file
  EXPECT_EQ(-1, hdfsAllowSnapshot(fs, path.c_str()));
  EXPECT_EQ((int) std::errc::not_a_directory, errno);
  EXPECT_EQ(-1, hdfsCreateSnapshot(fs, path.c_str(), "Bad"));
  EXPECT_EQ((int) std::errc::not_a_directory, errno);
  EXPECT_EQ(-1, hdfsDeleteSnapshot(fs, path.c_str(), "Bad"));
  EXPECT_EQ((int) std::errc::not_a_directory, errno);
  EXPECT_EQ(-1, hdfsRenameSnapshot(fs, path.c_str(), "Bad", "Bad"));
  EXPECT_EQ((int) std::errc::not_a_directory, errno);
  EXPECT_EQ(-1, hdfsDisallowSnapshot(fs, path.c_str()));
  EXPECT_EQ((int) std::errc::not_a_directory, errno);

  //Not snapshottable directory
  std::string dirName = connection.newDir();
  EXPECT_EQ(0, hdfsDisallowSnapshot(fs, dirName.c_str()));
  EXPECT_EQ(-1, hdfsCreateSnapshot(fs, dirName.c_str(), "Bad"));
  EXPECT_EQ((int) std::errc::invalid_argument, errno);

  //Verify snapshot created
  EXPECT_EQ(0, hdfsAllowSnapshot(fs, dirName.c_str()));
  EXPECT_EQ(0, hdfsCreateSnapshot(fs, dirName.c_str(), "Good"));
  std::string snapDir = dirName + ".snapshot/";
  int size;
  hdfsFileInfo *file_infos;
  EXPECT_NE(nullptr, file_infos = hdfsListDirectory(fs, snapDir.c_str(), &size));
  EXPECT_EQ(1, size);
  EXPECT_STREQ("Good", file_infos[0].mName);
  hdfsFreeFileInfo(file_infos, 1);

  //Verify snapshot renamed
  EXPECT_EQ(0, hdfsRenameSnapshot(fs, dirName.c_str(), "Good", "Best"));

  //Verify snapshot deleted
  EXPECT_EQ(0, hdfsDeleteSnapshot(fs, dirName.c_str(), "Best"));
  EXPECT_EQ(nullptr, file_infos = hdfsListDirectory(fs, snapDir.c_str(), &size));
  EXPECT_EQ(0, size);
  hdfsFreeFileInfo(file_infos, 0);
}

//Testing creating directories
TEST_F(HdfsExtTest, TestMkdirs) {
  HdfsHandle connection = cluster.connect_c();
  hdfsFS fs = connection.handle();
  EXPECT_NE(nullptr, fs);

  //Correct operation
  EXPECT_EQ(0, hdfsCreateDirectory(fs, "/myDir123"));

  //TODO Should return error if directory already exists?
  //EXPECT_EQ(-1, hdfsCreateDirectory(fs, "/myDir123"));
  //EXPECT_EQ((int) std::errc::file_exists, errno);

  //Creating directory on a path of the existing file
  std::string path = connection.newFile(1024); //1024 byte file
  EXPECT_EQ(-1, hdfsCreateDirectory(fs, path.c_str()));
  EXPECT_EQ((int) std::errc::file_exists, errno);
}

//Testing deleting files and directories
TEST_F(HdfsExtTest, TestDelete) {
  HdfsHandle connection = cluster.connect_c();
  hdfsFS fs = connection.handle();
  EXPECT_NE(nullptr, fs);

  //Path not found
  EXPECT_EQ(-1, hdfsDelete(fs, "/wrong_path", 1));
  EXPECT_EQ((int) std::errc::no_such_file_or_directory, errno);

  EXPECT_EQ(0, hdfsCreateDirectory(fs, "/myDir"));
  std::string path = connection.newFile("/myDir", 1024); //1024 byte file

  //Non-recursive delete should fail on a non-empty directory
  //error ENOTEMPTY(39) for libhdfspp or 255 for libhdfs
  EXPECT_EQ(-1, hdfsDelete(fs, "/myDir", 0));
  EXPECT_EQ((int) std::errc::directory_not_empty, errno);

  //Correct operation
  EXPECT_EQ(0, hdfsDelete(fs, "/myDir", 1));
}

//Testing renaming files and directories
TEST_F(HdfsExtTest, TestRename) {
  HdfsHandle connection = cluster.connect_c();
  hdfsFS fs = connection.handle();
  EXPECT_NE(nullptr, fs);

  //Creating directory with two files
  EXPECT_EQ(0, hdfsCreateDirectory(fs, "/myDir"));
  std::string file1 = connection.newFile("/myDir", 1024); //1024 byte file
  std::string file2 = connection.newFile("/myDir", 1024); //1024 byte file
  std::string file3 = connection.newFile(1024); //1024 byte file

  //Path not found
  EXPECT_EQ(-1, hdfsRename(fs, "/wrong_path", "/new_name"));
  EXPECT_EQ((int) std::errc::invalid_argument, errno);

  //No parent directory in new path
  EXPECT_EQ(-1, hdfsRename(fs, file1.c_str(), "/wrong_parent/new_name"));
  EXPECT_EQ((int ) std::errc::invalid_argument, errno);

  //New name already exists in the folder
  EXPECT_EQ(-1, hdfsRename(fs, file1.c_str(), file2.c_str()));
  EXPECT_EQ((int ) std::errc::invalid_argument, errno);

  //Correct operation
  EXPECT_EQ(0, hdfsRename(fs, file1.c_str(), "/myDir/new_awesome_name"));
  EXPECT_EQ(0, hdfsRename(fs, file3.c_str(), "/myDir/another_file"));
  EXPECT_EQ(0, hdfsRename(fs, "/myDir", "/new_awesome_dir"));

  //Verification
  int numEntries;
  hdfsFileInfo * dirList = hdfsListDirectory(fs, "/new_awesome_dir", &numEntries);
  EXPECT_NE(nullptr, dirList);
  EXPECT_EQ(3, numEntries);
  hdfsFreeFileInfo(dirList, 3);
}

//Testing Chmod and Chown
TEST_F(HdfsExtTest, TestChmodChown) {
  HdfsHandle connection = cluster.connect_c();
  hdfsFS fs = connection.handle();
  EXPECT_NE(nullptr, fs);

  //Path not found
  std::string path = "/wrong/dir/";
  EXPECT_EQ(-1, hdfsChmod(fs, path.c_str(), 0777));
  EXPECT_EQ((int ) std::errc::no_such_file_or_directory, errno);
  EXPECT_EQ(-1, hdfsChown(fs, path.c_str(), "foo", "bar"));
  EXPECT_EQ((int ) std::errc::no_such_file_or_directory, errno);

  //Wrong arguments
  path = connection.newFile(1024); //1024 byte file
  EXPECT_EQ(-1, hdfsChmod(fs, nullptr, 0777));
  EXPECT_EQ((int ) std::errc::invalid_argument, errno);
  EXPECT_EQ(-1, hdfsChmod(fs, path.c_str(), 07777));
  EXPECT_EQ((int ) std::errc::invalid_argument, errno);
  EXPECT_EQ(-1, hdfsChmod(fs, path.c_str(), -1));
  EXPECT_EQ((int ) std::errc::invalid_argument, errno);
  EXPECT_EQ(-1, hdfsChown(fs, nullptr, "foo", "bar"));
  EXPECT_EQ((int ) std::errc::invalid_argument, errno);

  //Permission denied
  HdfsHandle connection2 = cluster.connect_c("OtherGuy");
  hdfsFS fs2 = connection2.handle();
  EXPECT_EQ(-1, hdfsChmod(fs2, path.c_str(), 0123));
  EXPECT_EQ((int ) std::errc::permission_denied, errno);
  EXPECT_EQ(-1, hdfsChown(fs2, path.c_str(), "cool", "nice"));
  EXPECT_EQ((int ) std::errc::permission_denied, errno);

  //Verify Chmod and Chown worked
  EXPECT_EQ(0, hdfsChmod(fs, path.c_str(), 0123));
  EXPECT_EQ(0, hdfsChown(fs, path.c_str(), "cool", "nice"));
  hdfsFileInfo *file_info;
  EXPECT_NE(nullptr, file_info = hdfsGetPathInfo(fs, path.c_str()));
  EXPECT_EQ(0123, file_info->mPermissions);
  EXPECT_STREQ("cool", file_info->mOwner);
  EXPECT_STREQ("nice", file_info->mGroup);
  hdfsFreeFileInfo(file_info, 1);
}

//Testing EOF
TEST_F(HdfsExtTest, TestEOF) {
  HdfsHandle connection = cluster.connect_c();
  hdfsFS fs = connection.handle();
  EXPECT_NE(nullptr, fs);
  //Write to a file
  errno = 0;
  int size = 256;
  std::string path = "/eofTest";
  hdfsFile file = hdfsOpenFile(fs, path.c_str(), O_WRONLY, 0, 0, 0);
  EXPECT_NE(nullptr, file);
  void * buf = malloc(size);
  memset(buf, ' ', size);
  EXPECT_EQ(size, hdfsWrite(fs, file, buf, size));
  free(buf);
  EXPECT_EQ(0, hdfsCloseFile(fs, file));
  //libhdfs file operations work, but sometimes sets errno ENOENT : 2

  //Test normal reading (no EOF)
  char buffer[300];
  file = hdfsOpenFile(fs, path.c_str(), O_RDONLY, 0, 0, 0);
  EXPECT_EQ(size, hdfsPread(fs, file, 0, buffer, sizeof(buffer)));
  //Read executes correctly, but causes a warning (captured in HDFS-10595)
  //and sets errno to EINPROGRESS 115 : Operation now in progress

  //Test reading at offset past the EOF
  EXPECT_EQ(-1, hdfsPread(fs, file, sizeof(buffer), buffer, sizeof(buffer)));
  EXPECT_EQ(Status::kInvalidOffset, errno);

  EXPECT_EQ(0, hdfsCloseFile(fs, file));
}

//Testing hdfsExists
TEST_F(HdfsExtTest, TestExists) {

  HdfsHandle connection = cluster.connect_c();
  hdfsFS fs = connection.handle();
  EXPECT_NE(nullptr, fs);
  //Path not found
  EXPECT_EQ(-1, hdfsExists(fs, "/wrong/dir/"));
  EXPECT_EQ((int ) std::errc::no_such_file_or_directory, errno);

  //Correct operation
  std::string pathDir = "/testExistsDir";
  EXPECT_EQ(0, hdfsCreateDirectory(fs, pathDir.c_str()));
  EXPECT_EQ(0, hdfsExists(fs, pathDir.c_str()));
  std::string pathFile = connection.newFile(pathDir.c_str(), 1024);
  EXPECT_EQ(0, hdfsExists(fs, pathFile.c_str()));

  //Permission denied
  EXPECT_EQ(0, hdfsChmod(fs, pathDir.c_str(), 0700));
  HdfsHandle connection2 = cluster.connect_c("OtherGuy");
  hdfsFS fs2 = connection2.handle();
  EXPECT_EQ(-1, hdfsExists(fs2, pathFile.c_str()));
  EXPECT_EQ((int ) std::errc::permission_denied, errno);
}

//Testing Replication and Time modifications
TEST_F(HdfsExtTest, TestReplAndTime) {
  HdfsHandle connection = cluster.connect_c();
  hdfsFS fs = connection.handle();
  EXPECT_NE(nullptr, fs);

  std::string path = "/wrong/dir/";

  //Path not found
  EXPECT_EQ(-1, hdfsSetReplication(fs, path.c_str(), 3));
  EXPECT_EQ((int ) std::errc::no_such_file_or_directory, errno);
  EXPECT_EQ(-1, hdfsUtime(fs, path.c_str(), 1000000, 1000000));
  EXPECT_EQ((int ) std::errc::no_such_file_or_directory, errno);

  //Correct operation
  path = connection.newFile(1024);
  EXPECT_EQ(0, hdfsSetReplication(fs, path.c_str(), 7));
  EXPECT_EQ(0, hdfsUtime(fs, path.c_str(), 123456789, 987654321));
  hdfsFileInfo *file_info;
  EXPECT_NE(nullptr, file_info = hdfsGetPathInfo(fs, path.c_str()));
  EXPECT_EQ(7, file_info->mReplication);
  EXPECT_EQ(123456789, file_info->mLastMod);
  EXPECT_EQ(987654321, file_info->mLastAccess);
  hdfsFreeFileInfo(file_info, 1);

  //Wrong arguments
  EXPECT_EQ(-1, hdfsSetReplication(fs, path.c_str(), 0));
  EXPECT_EQ((int ) std::errc::invalid_argument, errno);
  EXPECT_EQ(-1, hdfsSetReplication(fs, path.c_str(), 513));
  EXPECT_EQ((int ) std::errc::invalid_argument, errno);

  //Permission denied
  EXPECT_EQ(0, hdfsChmod(fs, path.c_str(), 0700));
  HdfsHandle connection2 = cluster.connect_c("OtherGuy");
  hdfsFS fs2 = connection2.handle();
  EXPECT_EQ(-1, hdfsSetReplication(fs2, path.c_str(), 3));
  EXPECT_EQ((int ) std::errc::permission_denied, errno);
  EXPECT_EQ(-1, hdfsUtime(fs2, path.c_str(), 111111111, 222222222));
  EXPECT_EQ((int ) std::errc::permission_denied, errno);
}

//Testing getting default block size at path
TEST_F(HdfsExtTest, TestDefaultBlockSize) {
  HdfsHandle connection = cluster.connect_c();
  hdfsFS fs = connection.handle();
  EXPECT_NE(nullptr, fs);

  //Correct operation (existing path)
  std::string path = connection.newFile(1024);
  long block_size = hdfsGetDefaultBlockSizeAtPath(fs, path.c_str());
  EXPECT_GT(block_size, 0);
  hdfsFileInfo *file_info;
  EXPECT_NE(nullptr, file_info = hdfsGetPathInfo(fs, path.c_str()));
  EXPECT_EQ(block_size, file_info->mBlockSize);
  hdfsFreeFileInfo(file_info, 1);

  //Non-existing path
  path = "/wrong/dir/";
  EXPECT_GT(hdfsGetDefaultBlockSizeAtPath(fs, path.c_str()), 0);

  //No path specified
  EXPECT_GT(hdfsGetDefaultBlockSize(fs), 0);
}

//Testing getting hosts
TEST_F(HdfsExtTest, TestHosts) {
  HdfsHandle connection = cluster.connect_c();
  hdfsFS fs = connection.handle();
  EXPECT_NE(nullptr, fs);

  char *** hosts = nullptr;

  // Free a null pointer
  hdfsFreeHosts(hosts);
  EXPECT_EQ(0, errno);

  // Test non-existent files
  EXPECT_EQ(nullptr, hdfsGetHosts(fs, "/wrong/file/", 0, std::numeric_limits<int64_t>::max()));
  EXPECT_EQ((int ) std::errc::no_such_file_or_directory, errno);

  // Test an existent file
  std::string filename = connection.newFile(1024);
  EXPECT_NE(nullptr, hosts = hdfsGetHosts(fs, filename.c_str(), 0, std::numeric_limits<int64_t>::max()));

  //Make sure there is at least one host
  EXPECT_NE(nullptr, *hosts);
  EXPECT_NE(nullptr, **hosts);

  hdfsFreeHosts(hosts);
  EXPECT_EQ(0, errno);

  //Test invalid arguments
  EXPECT_EQ(nullptr, hdfsGetHosts(fs, filename.c_str(), 0, std::numeric_limits<int64_t>::min()));
  EXPECT_EQ((int) std::errc::invalid_argument, errno);

  //Test invalid arguments
  EXPECT_EQ(nullptr, hdfsGetHosts(fs, filename.c_str(), std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max()));
  EXPECT_EQ((int) std::errc::invalid_argument, errno);
}

//Testing read statistics
TEST_F(HdfsExtTest, TestReadStats) {
  HdfsHandle connection = cluster.connect_c();
  hdfsFS fs = connection.handle();
  EXPECT_NE(nullptr, fs);

  struct hdfsReadStatistics *stats;

  //Write to a file
  int size = 256;
  std::string path = "/readStatTest";
  hdfsFile file = hdfsOpenFile(fs, path.c_str(), O_WRONLY, 0, 0, 0);
  EXPECT_NE(nullptr, file);
  void * buf = malloc(size);
  explicit_bzero(buf, size);
  EXPECT_EQ(size, hdfsWrite(fs, file, buf, size));
  free(buf);
  EXPECT_EQ(0, hdfsCloseFile(fs, file));

  //test before reading
  file = hdfsOpenFile(fs, path.c_str(), O_RDONLY, 0, 0, 0);
  EXPECT_EQ(0, hdfsFileGetReadStatistics(file, &stats));
  EXPECT_EQ(0, stats->totalBytesRead);
  hdfsFileFreeReadStatistics(stats);

  //test after reading
  char buffer[123];
  //Read executes correctly, but causes a warning (captured in HDFS-10595)
  EXPECT_EQ(sizeof(buffer), hdfsRead(fs, file, buffer, sizeof(buffer)));
  EXPECT_EQ(0, hdfsFileGetReadStatistics(file, &stats));
  EXPECT_EQ(sizeof(buffer), stats->totalBytesRead);
  EXPECT_EQ(sizeof(buffer), stats->totalLocalBytesRead);
  EXPECT_EQ(0, hdfsReadStatisticsGetRemoteBytesRead(stats));
  hdfsFileFreeReadStatistics(stats);

  //test after clearing
  EXPECT_EQ(0, hdfsFileClearReadStatistics(file));
  EXPECT_EQ(0, hdfsFileGetReadStatistics(file, &stats));
  EXPECT_EQ(0, stats->totalBytesRead);
  hdfsFileFreeReadStatistics(stats);

  EXPECT_EQ(0, hdfsCloseFile(fs, file));
  // Since libhdfs is not guaranteed to set errno to 0 on successful
  // operations, we disable this check for now, see HDFS-14325 for a
  // long term solution to this problem
  // EXPECT_EQ(0, errno);
}

//Testing working directory
TEST_F(HdfsExtTest, TestWorkingDirectory) {
  HdfsHandle connection = cluster.connect_c();
  hdfsFS fs = connection.handle();
  EXPECT_NE(nullptr, fs);

  //Correct operation of setter and getter
  std::string pathDir = "/testWorkDir/";
  EXPECT_EQ(0, hdfsCreateDirectory(fs, pathDir.c_str()));
  std::string pathFile = connection.newFile(pathDir.c_str(), 1024);
  EXPECT_EQ(0, hdfsSetWorkingDirectory(fs, pathDir.c_str()));
  char array[100];
  EXPECT_STREQ(pathDir.c_str(), hdfsGetWorkingDirectory(fs, array, 100));

  //Get relative path
  std::size_t slashPos = pathFile.find_last_of("/");
  std::string fileName = pathFile.substr(slashPos + 1);

  //Testing various functions with relative path:

  //hdfsGetDefaultBlockSizeAtPath
  EXPECT_GT(hdfsGetDefaultBlockSizeAtPath(fs, fileName.c_str()), 0);

  //hdfsSetReplication
  EXPECT_EQ(0, hdfsSetReplication(fs, fileName.c_str(), 7));

  //hdfsUtime
  EXPECT_EQ(0, hdfsUtime(fs, fileName.c_str(), 123456789, 987654321));

  //hdfsExists
  EXPECT_EQ(0, hdfsExists(fs, fileName.c_str()));

  //hdfsGetPathInfo
  hdfsFileInfo *file_info;
  EXPECT_NE(nullptr, file_info = hdfsGetPathInfo(fs, fileName.c_str()));
  hdfsFreeFileInfo(file_info, 1);

  //hdfsOpenFile
  hdfsFile file;
  file = hdfsOpenFile(fs, fileName.c_str(), O_RDONLY, 0, 0, 0);
  EXPECT_EQ(0, hdfsCloseFile(fs, file));

  //hdfsCreateDirectory
  EXPECT_EQ(0, hdfsCreateDirectory(fs, "newDir"));

  //add another file
  std::string fileName2 = connection.newFile(pathDir + "/newDir", 1024);

  //hdfsListDirectory
  int numEntries;
  hdfsFileInfo * dirList;
  EXPECT_NE(nullptr, dirList = hdfsListDirectory(fs, "newDir", &numEntries));
  EXPECT_EQ(1, numEntries);
  hdfsFreeFileInfo(dirList, 1);

  //hdfsChmod
  EXPECT_EQ(0, hdfsChmod(fs, fileName.c_str(), 0777));

  //hdfsChown
  EXPECT_EQ(0, hdfsChown(fs, fileName.c_str(), "cool", "nice"));

  //hdfsDisallowSnapshot
  EXPECT_EQ(0, hdfsDisallowSnapshot(fs, "newDir"));

  //hdfsAllowSnapshot
  EXPECT_EQ(0, hdfsAllowSnapshot(fs, "newDir"));

  //hdfsCreateSnapshot
  EXPECT_EQ(0, hdfsCreateSnapshot(fs, "newDir", "Some"));

  //hdfsDeleteSnapshot
  EXPECT_EQ(0, hdfsDeleteSnapshot(fs, "newDir", "Some"));

  //hdfsGetBlockLocations
  hdfsBlockLocations * blocks = nullptr;
  EXPECT_EQ(0, hdfsGetBlockLocations(connection, fileName.c_str(), &blocks));
  hdfsFreeBlockLocations(blocks);

  //hdfsGetHosts
  char *** hosts;
  EXPECT_NE(nullptr, hosts = hdfsGetHosts(fs, fileName.c_str(), 0, std::numeric_limits<int64_t>::max()));
  hdfsFreeHosts(hosts);

  //hdfsRename
  EXPECT_EQ(0, hdfsRename(fs, fileName.c_str(), "new_file_name"));

  //hdfsDelete
  EXPECT_EQ(0, hdfsDelete(fs, "new_file_name", 0));
}


// Flags used to test event handlers
static int connect_callback_invoked = 0;
int basic_fs_callback(const char *event, const char *cluster, int64_t value, int64_t cookie) {
  (void)cluster;
  (void)value;
  if(::strstr(FS_NN_CONNECT_EVENT, event) && cookie == 0xFFF0) {
    connect_callback_invoked = 1;
  }
  return LIBHDFSPP_EVENT_OK;
}

// Make sure event handler gets called during connect
TEST_F(HdfsExtTest, TestConnectEvent) {
  connect_callback_invoked = 0;
  hdfsPreAttachFSMonitor(basic_fs_callback, 0xFFF0);

  HdfsHandle connection = cluster.connect_c();
  hdfsFS fs = connection.handle();
  EXPECT_NE(nullptr, fs);
  EXPECT_EQ(connect_callback_invoked, 1);
}

int throwing_fs_callback(const char *event, const char *cluster, int64_t value, int64_t cookie) {
  (void)cluster;
  (void)value;
  if(::strstr(FS_NN_CONNECT_EVENT, event) && cookie == 0xFFF1) {
    connect_callback_invoked = 1;
    throw std::runtime_error("Throwing in callbacks is a bad thing.");
  }
  return LIBHDFSPP_EVENT_OK;
}

// Make sure throwing in the connect event handler doesn't prevent connection
TEST_F(HdfsExtTest, TestConnectEventThrow) {
  connect_callback_invoked = 0;
  hdfsPreAttachFSMonitor(throwing_fs_callback, 0xFFF1);

  HdfsHandle connection = cluster.connect_c();
  hdfsFS fs = connection.handle();
  EXPECT_NE(nullptr, fs);
  EXPECT_EQ(connect_callback_invoked, 1);
}

int char_throwing_fs_callback(const char *event, const char *cluster, int64_t value, int64_t cookie) {
  (void)cluster;
  (void)value;
  if(::strstr(FS_NN_CONNECT_EVENT, event) && cookie == 0xFFF2) {
    connect_callback_invoked = 1;
    throw "Throwing non std::exceptions in callbacks is even worse.";
  }
  return LIBHDFSPP_EVENT_OK;
}

TEST_F(HdfsExtTest, TestConnectEventThrowChar) {
  connect_callback_invoked = 0;
  hdfsPreAttachFSMonitor(char_throwing_fs_callback, 0xFFF2);

  HdfsHandle connection = cluster.connect_c();
  hdfsFS fs = connection.handle();
  EXPECT_NE(nullptr, fs);
  EXPECT_EQ(connect_callback_invoked, 1);
}

// Make sure throwing in the read event handler doesn't prevent reads
int read_handler_invokation_count = 0;
int basic_read_event_handler(const char *event, const char *cluster, const char *file,
                             int64_t value, int64_t cookie)
{
  (void)cluster;
  (void)file;
  (void)value;
  if(::strstr(FILE_DN_READ_EVENT, event) && cookie == 0xFFF3) {
    read_handler_invokation_count += 1;
  }
  return LIBHDFSPP_EVENT_OK;
}

// Testing that read handler is called.
// Note: This is counting calls to async_read rather than hdfsPread.
//  Typically a call to hdfs(P)Read that doesn't span blocks/packets
//  invokes async_read 6 times; 4 more than required (improving that
//  in HDFS-11266).
TEST_F(HdfsExtTest, TestReadEvent) {
  read_handler_invokation_count = 0;
  hdfsPreAttachFileMonitor(basic_read_event_handler, 0xFFF3);

  HdfsHandle connection = cluster.connect_c();
  hdfsFS fs = connection.handle();
  EXPECT_NE(nullptr, fs);
  //Write to a file
  errno = 0;
  int size = 256;
  std::string path = "/readEventTest";
  hdfsFile file = hdfsOpenFile(fs, path.c_str(), O_WRONLY, 0, 0, 0);
  EXPECT_NE(nullptr, file);
  void * buf = malloc(size);
  memset(buf, ' ', size);
  EXPECT_EQ(size, hdfsWrite(fs, file, buf, size));
  free(buf);
  EXPECT_EQ(0, hdfsCloseFile(fs, file));

  //Test that read counters are getting incremented
  char buffer[300];
  file = hdfsOpenFile(fs, path.c_str(), O_RDONLY, 0, 0, 0);
  EXPECT_EQ(size, hdfsPread(fs, file, 0, buffer, sizeof(buffer)));
  EXPECT_EQ(read_handler_invokation_count, 6);

  EXPECT_EQ(size, hdfsPread(fs, file, 0, buffer, sizeof(buffer)));
  EXPECT_EQ(read_handler_invokation_count, 12);


  EXPECT_EQ(0, hdfsCloseFile(fs, file));
}

int throwing_read_event_handler(const char *event, const char *cluster, const char *file,
                             int64_t value, int64_t cookie)
{
  (void)cluster;
  (void)file;
  (void)value;
  if(::strstr(FILE_DN_READ_EVENT, event) && cookie == 0xFFF4) {
    read_handler_invokation_count += 1;
    throw std::runtime_error("Throwing here is a bad idea, but shouldn't break reads");
  }
  return LIBHDFSPP_EVENT_OK;
}

// Testing that reads can be done when event handler throws.
TEST_F(HdfsExtTest, TestReadEventThrow) {
  read_handler_invokation_count = 0;
  hdfsPreAttachFileMonitor(throwing_read_event_handler, 0xFFF4);

  HdfsHandle connection = cluster.connect_c();
  hdfsFS fs = connection.handle();
  EXPECT_NE(nullptr, fs);
  //Write to a file
  errno = 0;
  int size = 256;
  std::string path = "/readEventTest";
  hdfsFile file = hdfsOpenFile(fs, path.c_str(), O_WRONLY, 0, 0, 0);
  EXPECT_NE(nullptr, file);
  void * buf = malloc(size);
  memset(buf, ' ', size);
  EXPECT_EQ(size, hdfsWrite(fs, file, buf, size));
  free(buf);
  EXPECT_EQ(0, hdfsCloseFile(fs, file));

  //Test that read counters are getting incremented
  char buffer[300];
  file = hdfsOpenFile(fs, path.c_str(), O_RDONLY, 0, 0, 0);
  EXPECT_EQ(size, hdfsPread(fs, file, 0, buffer, sizeof(buffer)));
  EXPECT_EQ(read_handler_invokation_count, 6);

  EXPECT_EQ(size, hdfsPread(fs, file, 0, buffer, sizeof(buffer)));
  EXPECT_EQ(read_handler_invokation_count, 12);


  EXPECT_EQ(0, hdfsCloseFile(fs, file));
}


} // end namespace hdfs

int main(int argc, char *argv[]) {
  // The following line must be executed to initialize Google Mock
  // (and Google Test) before running the tests.
  ::testing::InitGoogleMock(&argc, argv);
  int exit_code = RUN_ALL_TESTS();
  google::protobuf::ShutdownProtobufLibrary();

  return exit_code;
}
