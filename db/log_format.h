// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Log format information shared by reader and writer.
// See ../doc/log_format.txt for more detail.

#ifndef STORAGE_LEVELDB_DB_LOG_FORMAT_H_
#define STORAGE_LEVELDB_DB_LOG_FORMAT_H_

namespace leveldb {
namespace log {

enum RecordType { // NOTE:htt, WAL日志文件中记录的类型
  // Zero is reserved for preallocated files
  kZeroType = 0,

  kFullType = 1,

  // For fragments
  kFirstType = 2,
  kMiddleType = 3,
  kLastType = 4
};
static const int kMaxRecordType = kLastType; // NOTE:htt, 最大记录类型

static const int kBlockSize = 32768; // NOTE:htt, WAL日志分块大小32k

// Header is checksum (4 bytes), type (1 byte), length (2 bytes).
static const int kHeaderSize = 4 + 1 + 2; // NOTE:htt, WAL日志中记录的头长度${CheckSum}${RecordLen}${Type}

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_FORMAT_H_
