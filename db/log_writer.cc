// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <stdint.h>
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

Writer::Writer(WritableFile* dest)
    : dest_(dest),
      block_offset_(0) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc_[i] = crc32c::Value(&t, 1); // NOTE:htt, 生成WAL日志记录类型的crc32值
  }
}

Writer::~Writer() {
}

Status Writer::AddRecord(const Slice& slice) { // NOTE:htt, 将slice写入到WAL日志中,如果长度大于块,则分多个部分写入
  const char* ptr = slice.data();
  size_t left = slice.size();

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;
  bool begin = true;
  do {
    const int leftover = kBlockSize - block_offset_; // NOTE:htt, 块中剩余空间
    assert(leftover >= 0);
    if (leftover < kHeaderSize) { // NOTE:htt, 若块中剩余空间小于7,则剩余空间补0
      // Switch to a new block
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        assert(kHeaderSize == 7);
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      block_offset_ = 0; // NOTE:htt, 设置block块内偏移为0
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    const size_t avail = kBlockSize - block_offset_ - kHeaderSize; // NOTE:htt, 获取块剩余空间
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type;
    const bool end = (left == fragment_length);
    if (begin && end) { // NOTE:htt, 若 begin和end都在一个块中,则记录类型为kFullType
      type = kFullType;
    } else if (begin) { // NOTE:htt, 若仅 begin在一个快中,则记录类型为kFirstType
      type = kFirstType;
    } else if (end) { // NOTE:htt, 若仅 end在一个块中,则记录类型为kLastType
      type = kLastType;
    } else { // NOTE:htt, 否则都是 kMiddleType
      type = kMiddleType;
    }

    s = EmitPhysicalRecord(type, ptr, fragment_length);// NOTE:htt, 将记录头部以及记录写入dst中
    ptr += fragment_length; // NOTE:htt, 调整数据指针
    left -= fragment_length; // NOTE:htt, 调整数据剩余内容长度
    begin = false; // NOTE:htt, begin为false
  } while (s.ok() && left > 0);
  return s;
}

Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr, size_t n) { // NOTE:htt, 将记录头部以及记录写入dst中/*{{{*/
  assert(n <= 0xffff);  // Must fit in two bytes
  assert(block_offset_ + kHeaderSize + n <= kBlockSize);

  // Format the header
  char buf[kHeaderSize]; // NOTE:htt, 头部格式 ${crc}${n}${t}
  buf[4] = static_cast<char>(n & 0xff);
  buf[5] = static_cast<char>(n >> 8);
  buf[6] = static_cast<char>(t);

  // Compute the crc of the record type and the payload.
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, n);
  crc = crc32c::Mask(crc);                 // Adjust for storage // NOTE: htt, 返回crc的掩码
  EncodeFixed32(buf, crc); // NOTE:htt, 将crc写入buf[0-3]

  // Write the header and the payload
  Status s = dest_->Append(Slice(buf, kHeaderSize)); // NOTE:htt, 将头部内容写入记录
  if (s.ok()) {
    s = dest_->Append(Slice(ptr, n)); // NOTE:htt, 写入记录
    if (s.ok()) {
      s = dest_->Flush(); // NOTE:htt, flush文件
    }
  }
  block_offset_ += kHeaderSize + n; // NOTE:htt, 调整块内偏移
  return s;
}/*}}}*/

}  // namespace log
}  // namespace leveldb
