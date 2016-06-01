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

#ifndef HDFSPP_BLOCK_LOCATION_H
#define HDFSPP_BLOCK_LOCATION_H

namespace hdfs {

class DNInfo {
public:
  DNInfo() : xfer_port(-1), info_port(-1), IPC_port(-1), info_secure_port(-1) {}

  std::string getHostname() const {
    return hostname;
  }

  void setHostname(const std::string & hostname) {
    this->hostname = hostname;
  }

  std::string getIPAddr() const {
    return ip_addr;
  }

  void setIPAddr(const std::string & ip_addr) {
    this->ip_addr = ip_addr;
  }

  int getXferPort() const {
    return xfer_port;
  }

  void setXferPort(int xfer_port) {
    this->xfer_port = xfer_port;
  }

  int getInfoPort() const {
    return info_port;
  }

  void setInfoPort(int info_port) {
    this->info_port = info_port;
  }

  int getIPCPort() const {
    return IPC_port;
  }

  void setIPCPort(int IPC_port) {
    this->IPC_port = IPC_port;
  }

  int getInfoSecurePort() const {
    return info_secure_port;
  }

  void setInfoSecurePort(int info_secure_port) {
    this->info_secure_port = info_secure_port;
  }
private:
  std::string hostname;
  std::string ip_addr;
  int         xfer_port;
  int         info_port;
  int         IPC_port;
  int         info_secure_port;
};

class BlockLocation {
public:
    bool isCorrupt() const {
        return corrupt;
    }

    void setCorrupt(bool corrupt) {
        this->corrupt = corrupt;
    }

    int64_t getLength() const {
        return length;
    }

    void setLength(int64_t length) {
        this->length = length;
    }

    int64_t getOffset() const {
        return offset;
    }

    void setOffset(int64_t offset) {
        this->offset = offset;
    }

    const std::vector<DNInfo> & getDataNodes() const {
        return dn_info;
    }

    void setDataNodes(const std::vector<DNInfo> & dn_info) {
        this->dn_info = dn_info;
    }

private:
    bool corrupt;
    int64_t length;
    int64_t offset;  // Offset of the block in the file
    std::vector<DNInfo> dn_info; // Info about who stores each block
};

class FileBlockLocation {
public:
  uint64_t getFileLength() {
    return fileLength;
  }

  void setFileLength(uint64_t fileLength) {
    this->fileLength = fileLength;
  }

  bool isLastBlockComplete() const {
    return this->lastBlockComplete;
  }

  void setLastBlockComplete(bool lastBlockComplete) {
    this->lastBlockComplete = lastBlockComplete;
  }

  bool isUnderConstruction() const {
    return underConstruction;
  }

  void setUnderConstruction(bool underConstruction) {
    this->underConstruction = underConstruction;
  }

  const std::vector<BlockLocation> & getBlockLocations() const {
    return blockLocations;
  }

  void setBlockLocations(const std::vector<BlockLocation> & blockLocations) {
    this->blockLocations = blockLocations;
  }
private:
  uint64_t fileLength;
  bool     lastBlockComplete;
  bool     underConstruction;
  std::vector<BlockLocation> blockLocations;
};

} // namespace hdfs


#endif /* HDFSPP_BLOCK_LOCATION_H */
