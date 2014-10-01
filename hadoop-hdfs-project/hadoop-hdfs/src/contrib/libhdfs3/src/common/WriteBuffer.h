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

#ifndef _HDFS_LIBHDFS3_COMMON_WRITEBUFFER_H_
#define _HDFS_LIBHDFS3_COMMON_WRITEBUFFER_H_

#include <cassert>
#include <cstddef>
#include <cstring>
#include <stdint.h>
#include <vector>

#include <arpa/inet.h>

namespace hdfs {
namespace internal {

/**
 * a data buffer used to read and write.
 */
class WriteBuffer {
public:
    /**
     * Construct a empty buffer.
     * @throw nothrow
     */
    WriteBuffer();

    /**
     * Destroy a buffer.
     * @throw nothrow
     */
    ~WriteBuffer();

    /**
     * Write string into buffer.
     * Terminated '\0' will also be written into buffer.
     * @param str The string to be written.
     * @throw nothrow
     */
    void writeString(const char *str) {
        writeString(str, size);
    }

    /**
     * Write string into buffer with given position.
     * Terminated '\0' will also be written into buffer and the data after given position will be overwritten.
     * @param str The string to be written.
     * @param pos The given start position in buffer.
     * @throw nothrow
     */
    void writeString(const char *str, size_t pos) {
        write(str, strlen(str) + 1, pos);
    }

    /**
     * Write a vector into buffer.
     * @param bytes The data be written.
     * @param s The size of data.
     */
    void write(const void *bytes, size_t s) {
        write(bytes, s, size);
    }

    /**
     * Write a vector into buffer with given position.
     * The data after given position will be overwritten.
     * @param bytes The data be written.
     * @param s The size of data.
     * @param pos The given start position in buffer.
     */
    void write(const void *bytes, size_t s, size_t pos);

    /**
     * Write char into buffer.
     * @param value The char to be written.
     * @throw nothrow
     */
    void write(char value) {
        write(value, size);
    }

    /**
     * Write char into buffer with given position.
     * The data after given position will be overwritten.
     * @param value The char to be written.
     * @param pos The given start position in buffer.
     * @throw nothrow
     */
    void write(char value, size_t pos) {
        write(&value, sizeof(value));
    }

    /**
     * Convert the 16 bit integer into big endian and write into buffer.
     * @param value The integer to be written.
     * @throw nothrow
     */
    void writeBigEndian(int16_t value) {
        writeBigEndian(value, size);
    }

    /**
     * Convert the 16 bit integer into big endian and write into buffer with given position.
     * The data after given position will be overwritten.
     * @param value The integer to be written.
     * @param pos The given start position in buffer.
     * @throw nothrow
     */
    void writeBigEndian(int16_t value, size_t pos) {
        int16_t v = htons(value);
        write((const char *) &v, sizeof(v));
    }

    /**
     * Convert the 32 bit integer into big endian and write into buffer.
     * @param value The integer to be written.
     * @throw nothrow
     */
    void writeBigEndian(int32_t value) {
        writeBigEndian(value, size);
    }

    /**
     * Convert the 32 bit integer into big endian and write into buffer with given position.
     * The data after given position will be overwritten.
     * @param value The integer to be written.
     * @param pos The given start position in buffer.
     * @throw nothrow
     */
    void writeBigEndian(int32_t value, size_t pos) {
        int32_t v = htonl(value);
        write((const char *) &v, sizeof(v), pos);
    }

    /**
     * Convert the 32 bit integer into varint and write into buffer.
     * @param value The integer to be written.
     * @throw nothrow
     */
    void writeVarint32(int32_t value) {
        writeVarint32(value, size);
    }

    /**
     * Convert the 32 bit integer into varint and write into buffer with given position.
     * The data after given position will be overwritten.
     * @param value The integer to be written.
     * @param pos The given start position in buffer.
     * @throw nothrow
     */
    void writeVarint32(int32_t value, size_t pos);

    /**
     * Get the buffered data from given offset.
     * @param offset The size of bytes to be ignored from begin of buffer.
     * @return The buffered data, or NULL if offset is over the end of data.
     * @throw nothrow
     */
    const char *getBuffer(size_t offset) const {
        assert(offset <= size && offset < buffer.size());

        if (offset >= size) {
            return NULL;
        }

        return &buffer[offset];
    }

    /**
     * Get the total bytes in the buffer from offset.
     * @param offset The size of bytes to be ignored from begin of buffer.
     * @return The total bytes in the buffer from offset.
     * @throw nothrow
     */
    size_t getDataSize(size_t offset) const {
        assert(offset <= size);
        return size - offset;
    }

    /**
     * Allocate a region of buffer to caller.
     * Caller should copy the data into this region manually instead of calling Buffer's method.
     *      This method will set the current data size to offset + s, caller may need to reset it to correct value.
     * @param offset Expected offset in the buffer, the data after given offset will be overwritten.
     * @param s Allocate the size of byte.
     * @return The start address in the buffer from offset, or NULL if offset is over the end of data.
     * @throw nothrow
     */
    char *alloc(size_t offset, size_t s);

    /**
     * Allocate a region of buffer to caller from the end of current buffer.
     * Caller should copy the data into this region manually instead of calling Buffer's method.
     *      This method will set the current data size to size + s, caller may need to reset it to correct value.
     * @param s Allocate the size of byte.
     * @return The start address in the buffer from offset.
     * @throw nothrow
     */
    char *alloc(size_t s) {
        return alloc(size, s);
    }

    /**
     * Set the available data size.
     * @param s The size to be set.
     * throw nothrow
     */
    void setBufferDataSize(size_t s) {
        size = s;
    }

private:
    size_t size; //current write position.
    std::vector<char> buffer;

};

}
}
#endif /* _HDFS_LIBHDFS3_COMMON_WRITEBUFFER_H_ */
