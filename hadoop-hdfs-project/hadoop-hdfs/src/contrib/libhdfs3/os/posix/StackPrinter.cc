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

#include "StackPrinter.h"

#include <cassert>
#include <cxxabi.h>
#include <dlfcn.h>
#include <execinfo.h>
#include <sstream>
#include <string>
#include <vector>

namespace hdfs {
namespace internal {

static void ATTRIBUTE_NOINLINE GetStack(int skip, int maxDepth,
                                        std::vector<void *> & stack) {
    std::ostringstream ss;
    ++skip; //current frame.
    stack.resize(maxDepth + skip);
    int size;
    size = backtrace(&stack[0], maxDepth + skip);
    size = size - skip;

    if (size < 0) {
        stack.resize(0);
        return;
    }

    stack.erase(stack.begin(), stack.begin() + skip);
    stack.resize(size);
}

std::string DemangleSymbol(const char * symbol) {
    int status;
    std::string retval;
    char * name = abi::__cxa_demangle(symbol, 0, 0, &status);

    switch (status) {
    case 0:
        retval = name;
        break;

    case -1:
        throw std::bad_alloc();
        break;

    case -2:
        retval = symbol;
        break;

    case -3:
        retval = symbol;
        break;
    }

    if (name) {
        free(name);
    }

    return retval;
}

#if defined(__ELF__)

#include <elf.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <link.h>  // For ElfW() macro.
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

// Re-runs fn until it doesn't cause EINTR.
#define NO_INTR(fn)   do {} while ((fn) < 0 && errno == EINTR)

// Read up to "count" bytes from file descriptor "fd" into the buffer
// starting at "buf" while handling short reads and EINTR.  On
// success, return the number of bytes read.  Otherwise, return -1.
static ssize_t ReadPersistent(const int fd, void * buf, const size_t count) {
    assert(fd >= 0);
    char * buf0 = reinterpret_cast<char *>(buf);
    ssize_t num_bytes = 0;

    while (num_bytes < static_cast<ssize_t>(count)) {
        ssize_t len;
        NO_INTR(len = read(fd, buf0 + num_bytes, count - num_bytes));

        if (len < 0) {  // There was an error other than EINTR.
            return -1;
        }

        if (len == 0) {  // Reached EOF.
            break;
        }

        num_bytes += len;
    }

    return num_bytes;
}

// Read up to "count" bytes from "offset" in the file pointed by file
// descriptor "fd" into the buffer starting at "buf".  On success,
// return the number of bytes read.  Otherwise, return -1.
static ssize_t ReadFromOffset(const int fd, void * buf,
                              const size_t count, const off_t offset) {
    off_t off = lseek(fd, offset, SEEK_SET);

    if (off == (off_t) - 1) {
        return -1;
    }

    return ReadPersistent(fd, buf, count);
}

// Try reading exactly "count" bytes from "offset" bytes in a file
// pointed by "fd" into the buffer starting at "buf" while handling
// short reads and EINTR.  On success, return true. Otherwise, return
// false.
static bool ReadFromOffsetExact(const int fd, void * buf,
                                const size_t count, const off_t offset) {
    ssize_t len = ReadFromOffset(fd, buf, count, offset);
    return len == static_cast<ssize_t>(count);
}

// Returns elf_header.e_type if the file pointed by fd is an ELF binary.
static int FileGetElfType(const int fd) {
    ElfW(Ehdr) elf_header;

    if (!ReadFromOffsetExact(fd, &elf_header, sizeof(elf_header), 0)) {
        return -1;
    }

    if (memcmp(elf_header.e_ident, ELFMAG, SELFMAG) != 0) {
        return -1;
    }

    return elf_header.e_type;
}

// Read the section headers in the given ELF binary, and if a section
// of the specified type is found, set the output to this section header
// and return true.  Otherwise, return false.
// To keep stack consumption low, we would like this function to not get
// inlined.
static bool
GetSectionHeaderByType(const int fd, ElfW(Half) sh_num, const off_t sh_offset,
                       ElfW(Word) type, ElfW(Shdr) *out) {
    // Read at most 16 section headers at a time to save read calls.
    ElfW(Shdr) buf[16];

    for (int i = 0; i < sh_num;) {
        const ssize_t num_bytes_left = (sh_num - i) * sizeof(buf[0]);
        const ssize_t num_bytes_to_read =
            (sizeof(buf) > static_cast<size_t>(num_bytes_left)) ? num_bytes_left : sizeof(buf);
        const ssize_t len = ReadFromOffset(fd, buf, num_bytes_to_read,
                                           sh_offset + i * sizeof(buf[0]));
        assert(len % sizeof(buf[0]) == 0);
        const ssize_t num_headers_in_buf = len / sizeof(buf[0]);

        for (int j = 0; j < num_headers_in_buf; ++j) {
            if (buf[j].sh_type == type) {
                *out = buf[j];
                return true;
            }
        }

        i += num_headers_in_buf;
    }

    return false;
}

// There is no particular reason to limit section name to 63 characters,
// but there has (as yet) been no need for anything longer either.
const int kMaxSectionNameLen = 64;

// name_len should include terminating '\0'.
bool GetSectionHeaderByName(int fd, const char * name, size_t name_len,
                            ElfW(Shdr) *out) {
    ElfW(Ehdr) elf_header;

    if (!ReadFromOffsetExact(fd, &elf_header, sizeof(elf_header), 0)) {
        return false;
    }

    ElfW(Shdr) shstrtab;
    off_t shstrtab_offset = (elf_header.e_shoff +
                             elf_header.e_shentsize * elf_header.e_shstrndx);

    if (!ReadFromOffsetExact(fd, &shstrtab, sizeof(shstrtab), shstrtab_offset)) {
        return false;
    }

    for (int i = 0; i < elf_header.e_shnum; ++i) {
        off_t section_header_offset = (elf_header.e_shoff +
                                       elf_header.e_shentsize * i);

        if (!ReadFromOffsetExact(fd, out, sizeof(*out), section_header_offset)) {
            return false;
        }

        char header_name[kMaxSectionNameLen];

        if (sizeof(header_name) < name_len) {
            // No point in even trying.
            return false;
        }

        off_t name_offset = shstrtab.sh_offset + out->sh_name;
        ssize_t n_read = ReadFromOffset(fd, &header_name, name_len, name_offset);

        if (n_read == -1) {
            return false;
        } else if (n_read != static_cast<ssize_t>(name_len)) {
            // Short read -- name could be at end of file.
            continue;
        }

        if (memcmp(header_name, name, name_len) == 0) {
            return true;
        }
    }

    return false;
}

// Read a symbol table and look for the symbol containing the
// pc. Iterate over symbols in a symbol table and look for the symbol
// containing "pc".  On success, return true and write the symbol name
// to out.  Otherwise, return false.
// To keep stack consumption low, we would like this function to not get
// inlined.
static bool
FindSymbol(uint64_t pc, const int fd, char * out, int out_size,
           uint64_t symbol_offset, const ElfW(Shdr) *strtab,
           const ElfW(Shdr) *symtab) {
    if (symtab == NULL) {
        return false;
    }

    const int num_symbols = symtab->sh_size / symtab->sh_entsize;

    for (int i = 0; i < num_symbols;) {
        off_t offset = symtab->sh_offset + i * symtab->sh_entsize;
        // If we are reading Elf64_Sym's, we want to limit this array to
        // 32 elements (to keep stack consumption low), otherwise we can
        // have a 64 element Elf32_Sym array.
#if __WORDSIZE == 64
#define NUM_SYMBOLS 32
#else
#define NUM_SYMBOLS 64
#endif
        // Read at most NUM_SYMBOLS symbols at once to save read() calls.
        ElfW(Sym) buf[NUM_SYMBOLS];
        const ssize_t len = ReadFromOffset(fd, &buf, sizeof(buf), offset);
        assert(len % sizeof(buf[0]) == 0);
        const ssize_t num_symbols_in_buf = len / sizeof(buf[0]);

        for (int j = 0; j < num_symbols_in_buf; ++j) {
            const ElfW(Sym)& symbol = buf[j];
            uint64_t start_address = symbol.st_value;
            start_address += symbol_offset;
            uint64_t end_address = start_address + symbol.st_size;

            if (symbol.st_value != 0 &&  // Skip null value symbols.
                    symbol.st_shndx != 0 &&// Skip undefined symbols.
                    start_address <= pc && pc < end_address) {
                ssize_t len1 = ReadFromOffset(fd, out, out_size,
                                              strtab->sh_offset + symbol.st_name);

                if (len1 <= 0 || memchr(out, '\0', out_size) == NULL) {
                    return false;
                }

                return true;  // Obtained the symbol name.
            }
        }

        i += num_symbols_in_buf;
    }

    return false;
}

// Get the symbol name of "pc" from the file pointed by "fd".  Process
// both regular and dynamic symbol tables if necessary.  On success,
// write the symbol name to "out" and return true.  Otherwise, return
// false.
static bool GetSymbolFromObjectFile(const int fd, uint64_t pc,
                                    char * out, int out_size,
                                    uint64_t map_start_address) {
    // Read the ELF header.
    ElfW(Ehdr) elf_header;

    if (!ReadFromOffsetExact(fd, &elf_header, sizeof(elf_header), 0)) {
        return false;
    }

    uint64_t symbol_offset = 0;

    if (elf_header.e_type == ET_DYN) {  // DSO needs offset adjustment.
        symbol_offset = map_start_address;
    }

    ElfW(Shdr) symtab, strtab;

    // Consult a regular symbol table first.
    if (!GetSectionHeaderByType(fd, elf_header.e_shnum, elf_header.e_shoff,
                                SHT_SYMTAB, &symtab)) {
        return false;
    }

    if (!ReadFromOffsetExact(fd, &strtab, sizeof(strtab), elf_header.e_shoff +
                             symtab.sh_link * sizeof(symtab))) {
        return false;
    }

    if (FindSymbol(pc, fd, out, out_size, symbol_offset,
                   &strtab, &symtab)) {
        return true;  // Found the symbol in a regular symbol table.
    }

    // If the symbol is not found, then consult a dynamic symbol table.
    if (!GetSectionHeaderByType(fd, elf_header.e_shnum, elf_header.e_shoff,
                                SHT_DYNSYM, &symtab)) {
        return false;
    }

    if (!ReadFromOffsetExact(fd, &strtab, sizeof(strtab), elf_header.e_shoff +
                             symtab.sh_link * sizeof(symtab))) {
        return false;
    }

    if (FindSymbol(pc, fd, out, out_size, symbol_offset,
                   &strtab, &symtab)) {
        return true;  // Found the symbol in a dynamic symbol table.
    }

    return false;
}

namespace {
// Thin wrapper around a file descriptor so that the file descriptor
// gets closed for sure.
struct FileDescriptor {
    const int fd_;
    explicit FileDescriptor(int fd) : fd_(fd) {}
    ~FileDescriptor() {
        if (fd_ >= 0) {
            NO_INTR(close(fd_));
        }
    }
    int get() {
        return fd_;
    }

private:
    explicit FileDescriptor(const FileDescriptor &);
    void operator=(const FileDescriptor &);
};

// Helper class for reading lines from file.
//
// Note: we don't use ProcMapsIterator since the object is big (it has
// a 5k array member) and uses async-unsafe functions such as sscanf()
// and snprintf().
class LineReader {
public:
    explicit LineReader(int fd, char * buf, int buf_len) : fd_(fd),
        buf_(buf), buf_len_(buf_len), bol_(buf), eol_(buf), eod_(buf) {
    }

    // Read '\n'-terminated line from file.  On success, modify "bol"
    // and "eol", then return true.  Otherwise, return false.
    //
    // Note: if the last line doesn't end with '\n', the line will be
    // dropped.  It's an intentional behavior to make the code simple.
    bool ReadLine(const char ** bol, const char ** eol) {
        if (BufferIsEmpty()) {  // First time.
            const ssize_t num_bytes = ReadPersistent(fd_, buf_, buf_len_);

            if (num_bytes <= 0) {  // EOF or error.
                return false;
            }

            eod_ = buf_ + num_bytes;
            bol_ = buf_;
        } else {
            bol_ = eol_ + 1;  // Advance to the next line in the buffer.
            assert(bol_ <= eod_);// "bol_" can point to "eod_".

            if (!HasCompleteLine()) {
                const int incomplete_line_length = eod_ - bol_;
                // Move the trailing incomplete line to the beginning.
                memmove(buf_, bol_, incomplete_line_length);
                // Read text from file and append it.
                char * const append_pos = buf_ + incomplete_line_length;
                const int capacity_left = buf_len_ - incomplete_line_length;
                const ssize_t num_bytes = ReadPersistent(fd_, append_pos,
                                          capacity_left);

                if (num_bytes <= 0) {  // EOF or error.
                    return false;
                }

                eod_ = append_pos + num_bytes;
                bol_ = buf_;
            }
        }

        eol_ = FindLineFeed();

        if (eol_ == NULL) {  // '\n' not found.  Malformed line.
            return false;
        }

        *eol_ = '\0';  // Replace '\n' with '\0'.
        *bol = bol_;
        *eol = eol_;
        return true;
    }

    // Beginning of line.
    const char * bol() {
        return bol_;
    }

    // End of line.
    const char * eol() {
        return eol_;
    }

private:
    explicit LineReader(const LineReader &);
    void operator=(const LineReader &);

    char * FindLineFeed() {
        return reinterpret_cast<char *>(memchr(bol_, '\n', eod_ - bol_));
    }

    bool BufferIsEmpty() {
        return buf_ == eod_;
    }

    bool HasCompleteLine() {
        return !BufferIsEmpty() && FindLineFeed() != NULL;
    }

    const int fd_;
    char * const buf_;
    const int buf_len_;
    char * bol_;
    char * eol_;
    const char * eod_; // End of data in "buf_".
};
}  // namespace

// Place the hex number read from "start" into "*hex".  The pointer to
// the first non-hex character or "end" is returned.
static char * GetHex(const char * start, const char * end, uint64_t * hex) {
    *hex = 0;
    const char * p;

    for (p = start; p < end; ++p) {
        int ch = *p;

        if ((ch >= '0' && ch <= '9') ||
                (ch >= 'A' && ch <= 'F') || (ch >= 'a' && ch <= 'f')) {
            *hex = (*hex << 4) | (ch < 'A' ? ch - '0' : (ch & 0xF) + 9);
        } else {  // Encountered the first non-hex character.
            break;
        }
    }

    assert(p <= end);
    return const_cast<char *>(p);
}

// Search for the object file (from /proc/self/maps) that contains
// the specified pc. If found, open this file and return the file handle,
// and also set start_address to the start address of where this object
// file is mapped to in memory. Otherwise, return -1.
static int
OpenObjectFileContainingPcAndGetStartAddress(uint64_t pc,
        uint64_t & start_address) {
    int object_fd;
    // Open /proc/self/maps.
    int maps_fd;
    NO_INTR(maps_fd = open("/proc/self/maps", O_RDONLY));
    FileDescriptor wrapped_maps_fd(maps_fd);

    if (wrapped_maps_fd.get() < 0) {
        return -1;
    }

    // Iterate over maps and look for the map containing the pc.  Then
    // look into the symbol tables inside.
    char buf[1024];// Big enough for line of sane /proc/self/maps
    LineReader reader(wrapped_maps_fd.get(), buf, sizeof(buf));

    while (true) {
        const char * cursor;
        const char * eol;

        if (!reader.ReadLine(&cursor, &eol)) {  // EOF or malformed line.
            return -1;
        }

        // Start parsing line in /proc/self/maps.  Here is an example:
        //
        // 08048000-0804c000 r-xp 00000000 08:01 2142121    /bin/cat
        //
        // We want start address (08048000), end address (0804c000), flags
        // (r-xp) and file name (/bin/cat).
        // Read start address.
        cursor = GetHex(cursor, eol, &start_address);

        if (cursor == eol || *cursor != '-') {
            return -1;  // Malformed line.
        }

        ++cursor;  // Skip '-'.
        // Read end address.
        uint64_t end_address;
        cursor = GetHex(cursor, eol, &end_address);

        if (cursor == eol || *cursor != ' ') {
            return -1;  // Malformed line.
        }

        ++cursor;  // Skip ' '.

        // Check start and end addresses.
        if (!(start_address <= pc && pc < end_address)) {
            continue;  // We skip this map.  PC isn't in this map.
        }

        // Read flags.  Skip flags until we encounter a space or eol.
        const char * const flags_start = cursor;

        while (cursor < eol && *cursor != ' ') {
            ++cursor;
        }

        // We expect at least four letters for flags (ex. "r-xp").
        if (cursor == eol || cursor < flags_start + 4) {
            return -1;  // Malformed line.
        }

        // Check flags.  We are only interested in "r-x" maps.
        if (memcmp(flags_start, "r-x", 3) != 0) {  // Not a "r-x" map.
            continue;// We skip this map.
        }

        ++cursor;  // Skip ' '.
        // Skip to file name.  "cursor" now points to file offset.  We need to
        // skip at least three spaces for file offset, dev, and inode.
        int num_spaces = 0;

        while (cursor < eol) {
            if (*cursor == ' ') {
                ++num_spaces;
            } else if (num_spaces >= 3) {
                // The first non-space character after  skipping three spaces
                // is the beginning of the file name.
                break;
            }

            ++cursor;
        }

        if (cursor == eol) {
            return -1;  // Malformed line.
        }

        // Finally, "cursor" now points to file name of our interest.
        NO_INTR(object_fd = open(cursor, O_RDONLY));

        if (object_fd < 0) {
            return -1;
        }

        return object_fd;
    }
}

static const std::string SymbolizeAndDemangle(void * pc) {
    std::vector<char> buffer(1024);
    std::ostringstream ss;
    uint64_t pc0 = reinterpret_cast<uintptr_t>(pc);
    uint64_t start_address = 0;
    int object_fd = OpenObjectFileContainingPcAndGetStartAddress(pc0,
                    start_address);

    if (object_fd == -1) {
        return DEFAULT_STACK_PREFIX"Unknown";
    }

    FileDescriptor wrapped_object_fd(object_fd);
    int elf_type = FileGetElfType(wrapped_object_fd.get());

    if (elf_type == -1) {
        return DEFAULT_STACK_PREFIX"Unknown";
    }

    if (!GetSymbolFromObjectFile(wrapped_object_fd.get(), pc0,
                                 &buffer[0], buffer.size(), start_address)) {
        return DEFAULT_STACK_PREFIX"Unknown";
    }

    ss << DEFAULT_STACK_PREFIX << DemangleSymbol(&buffer[0]);
    return ss.str();
}

#elif defined(OS_MACOSX) && defined(HAVE_DLADDR)

static const std::string SymbolizeAndDemangle(void * pc) {
    Dl_info info;
    std::ostringstream ss;

    if (dladdr(pc, &info) && info.dli_sname) {
        ss << DEFAULT_STACK_PREFIX << DemangleSymbol(info.dli_sname);
    } else {
        ss << DEFAULT_STACK_PREFIX << "Unknown";
    }

    return ss.str();
}

#endif

const std::string PrintStack(int skip, int maxDepth) {
    std::ostringstream ss;
    std::vector<void *> stack;
    GetStack(skip + 1, maxDepth, stack);

    for (size_t i = 0; i < stack.size(); ++i) {
        ss << SymbolizeAndDemangle(stack[i]) << std::endl;
    }

    return ss.str();
}

}
}

