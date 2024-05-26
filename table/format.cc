// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/format.h"

#include "leveldb/env.h"
#include "port/port.h"
#include "table/block.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

void BlockHandle::EncodeTo(std::string* dst) const { // NOTE: htt, 将 offset_/size_ 以变长编码到 dst
  // Sanity check that all fields have been set
  assert(offset_ != ~static_cast<uint64_t>(0));
  assert(size_ != ~static_cast<uint64_t>(0));
  PutVarint64(dst, offset_); // NOTE: htt, 将 offset_ 以变长添加到 dst
  PutVarint64(dst, size_); // NOTE: htt, 将 size_ 以变长添加到 dst
}

Status BlockHandle::DecodeFrom(Slice* input) { // NOTE: htt, 从 input中获取 offset_ 和 size_
  if (GetVarint64(input, &offset_) && // NOTE: htt, 获取 offset_
      GetVarint64(input, &size_)) { // NOTE: htt, 获取 size_
    return Status::OK();
  } else {
    return Status::Corruption("bad block handle");
  }
}

void Footer::EncodeTo(std::string* dst) const { // NOTE: htt, 编码footer到 dst
#ifndef NDEBUG
  const size_t original_size = dst->size();
#endif
  metaindex_handle_.EncodeTo(dst); // NOTE: htt, 先把元信息索引
  index_handle_.EncodeTo(dst); // NOTE: htt, 在保存数据索引
  dst->resize(2 * BlockHandle::kMaxEncodedLength);  // Padding // TODO: htt, 这里resize()保证dst原来无数据，否则可能被阶段
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu)); // NOTE: htt, 添加魔数低32位
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32)); // NOTE: htt, 添加魔数高32位
  assert(dst->size() == original_size + kEncodedLength);
}

Status Footer::DecodeFrom(Slice* input) { // NOTE: htt, 解析footer，并将input指针前移
  const char* magic_ptr = input->data() + kEncodedLength - 8;
  const uint32_t magic_lo = DecodeFixed32(magic_ptr); // NOTE: htt, 获取魔数低32位
  const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4); // NOTE: htt, 获取魔数高32位
  const uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
                          (static_cast<uint64_t>(magic_lo)));
  if (magic != kTableMagicNumber) {
    return Status::InvalidArgument("not an sstable (bad magic number)");
  }

  Status result = metaindex_handle_.DecodeFrom(input); // NOTE: htt, 解析元信息索引
  if (result.ok()) {
    result = index_handle_.DecodeFrom(input); // NOTE: htt, 解析数据索引
  }
  if (result.ok()) {
    // We skip over any leftover data (just padding for now) in "input"
    const char* end = magic_ptr + 8;
    *input = Slice(end, input->data() + input->size() - end); // NOTE: htt, 跳过尾部，将input指针前移
  }
  return result;
}

Status ReadBlock(RandomAccessFile* file,
                 const ReadOptions& options,
                 const BlockHandle& handle,
                 BlockContents* result) { // NOTE: htt, 从 BlockHandle索引读取指向 <offset_, size_>数据
  result->data = Slice();
  result->cachable = false;
  result->heap_allocated = false;

  // Read the block contents as well as the type/crc footer.
  // See table_builder.cc for the code that built this structure.
  size_t n = static_cast<size_t>(handle.size());
  char* buf = new char[n + kBlockTrailerSize];
  Slice contents;
  Status s = file->Read(handle.offset(), n + kBlockTrailerSize, &contents, buf);
  if (!s.ok()) {
    delete[] buf;
    return s;
  }
  if (contents.size() != n + kBlockTrailerSize) {
    delete[] buf;
    return Status::Corruption("truncated block read");
  }

  // Check the crc of the type and the block contents
  const char* data = contents.data();    // Pointer to where Read put the data
  if (options.verify_checksums) { // NOTE: htt, 验证crc一致性
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1)); // NOTE: htt, 获取crc
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
      delete[] buf;
      s = Status::Corruption("block checksum mismatch");
      return s;
    }
  }

  switch (data[n]) {
    case kNoCompression: // NOTE: htt, 无压缩
      if (data != buf) {
        // File implementation gave us pointer to some other data.
        // Use it directly under the assumption that it will be live
        // while the file is open.
        delete[] buf;
        result->data = Slice(data, n); // NOTE: htt, 从其他内存获取数据，比如mmap地址
        result->heap_allocated = false;
        result->cachable = false;  // Do not double-cache // NOTE:htt, 如果是mmap等缓存,则不用对Block进行再次缓存
      } else {
        result->data = Slice(buf, n); // NOTE: htt, 使用 new buf[]空间
        result->heap_allocated = true;
        result->cachable = true;
      }

      // Ok
      break;
    case kSnappyCompression: { // NOTE; htt, 有压缩
      fprintf(stderr, "snappy decompression\n");
      size_t ulength = 0;
      if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
        delete[] buf;
        return Status::Corruption("corrupted compressed block contents");
      }
      char* ubuf = new char[ulength];
      if (!port::Snappy_Uncompress(data, n, ubuf)) {
        delete[] buf;
        delete[] ubuf;
        return Status::Corruption("corrupted compressed block contents");
      }
      delete[] buf;
      result->data = Slice(ubuf, ulength); // NOTE: htt, 有压缩情况，则直接使用新的内存空间
      result->heap_allocated = true;
      result->cachable = true;
      break;
    }
    case kZstdCompression: { // NOTE; htt, 有压缩
      fprintf(stderr, "zstd decompression\n");
      size_t ulength = 0;
      if (!port::Zstd_GetUncompressedLength(data, n, &ulength)) {
        delete[] buf;
        return Status::Corruption("corrupted compressed block contents");
      }
      char* ubuf = new char[ulength];
      if (!port::Zstd_Uncompress(data, n, ubuf, &ulength)) { // NOTE:htt, ulength可能会变化
        delete[] buf;
        delete[] ubuf;
        return Status::Corruption("corrupted compressed block contents");
      }
      delete[] buf;
      result->data = Slice(ubuf, ulength); // NOTE: htt, 有压缩情况，则直接使用新的内存空间
      result->heap_allocated = true;
      result->cachable = true;
      break;
    }
    default:
      delete[] buf;
      return Status::Corruption("bad block type");
  }

  return Status::OK();
}

}  // namespace leveldb
