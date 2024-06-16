// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_LOG_WRITER_H_
#define STORAGE_LEVELDB_DB_LOG_WRITER_H_

#include <stdint.h>
#include "db/log_format.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

class WritableFile;

namespace log {

// WAL日志按32K块严格划分Block
// 单条记录可能会跨多个Block,在每个Block会存储部分记录,并且会标记当前部分的类型,即RecordType
// 单个记录在单个block格式: ${crc}${n}${t}${str},
// 其中n为${str}长度,t为WAL中记录类型,str为写入记录,crc为${t}${str}的crc32
class Writer {  // NOTE:htt, 将记录写入WAL日志中,如果记录大于块长度,则拆分多个部分写入
 public:
  // Create a writer that will append data to "*dest".
  // "*dest" must be initially empty.
  // "*dest" must remain live while this Writer is in use.
  explicit Writer(WritableFile* dest);
  ~Writer();

  Status AddRecord(const Slice& slice);

 private:
  WritableFile* dest_;  // NOTE:htt, WAL日志文件实际的写入文件描述符
  int block_offset_;    // Current offset in block // NOTE:htt, block块空闲空间偏移

  // crc32c values for all supported record types.  These are
  // pre-computed to reduce the overhead of computing the crc of the
  // record type stored in the header.
  uint32_t type_crc_[kMaxRecordType + 1];  // NOTE:htt, 将WAL中记录每种类型都生成crc32值

  Status EmitPhysicalRecord(RecordType type, const char* ptr, size_t length);

  // No copying allowed
  Writer(const Writer&);
  void operator=(const Writer&);
};

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_WRITER_H_
