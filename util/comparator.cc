// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <stdint.h>
#include "leveldb/comparator.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "util/logging.h"

namespace leveldb {

Comparator::~Comparator() { }

namespace {
class BytewiseComparatorImpl : public Comparator { // NOTE: htt, Bytewise比较器
 public:
  BytewiseComparatorImpl() { }

  virtual const char* Name() const {
    return "leveldb.BytewiseComparator"; // NOTE: htt, Bytewise比较器名称
  }

  virtual int Compare(const Slice& a, const Slice& b) const {
    return a.compare(b); // NOTE: htt, 复用 slice的比较功能
  }

  virtual void FindShortestSeparator(
      std::string* start,
      const Slice& limit) const { // NOTE: htt, 找到 比start大，比limit小的最小字符串，如 abcg 和 abmn，则找到字符串为 abd
    // Find length of common prefix
    size_t min_length = std::min(start->size(), limit.size());
    size_t diff_index = 0;
    while ((diff_index < min_length) &&
           ((*start)[diff_index] == limit[diff_index])) {
      diff_index++; // NOTE: htt, 找到start 和 limit不一致的位置
    }

    if (diff_index >= min_length) {
      // Do not shorten if one string is a prefix of the other
    } else {
      uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);
      if (diff_byte < static_cast<uint8_t>(0xff) &&
          diff_byte + 1 < static_cast<uint8_t>(limit[diff_index])) {
        (*start)[diff_index]++; // NOTE: htt, 找到不一致位置，并将内容+1
        start->resize(diff_index + 1);
        assert(Compare(*start, limit) < 0);
      }
    }
  }

  virtual void FindShortSuccessor(std::string* key) const { // NOTE: htt, 找到比key大最小字符串，比如amn,对应字符串为b
    // Find first character that can be incremented
    size_t n = key->size();
    for (size_t i = 0; i < n; i++) {
      const uint8_t byte = (*key)[i];
      if (byte != static_cast<uint8_t>(0xff)) {
        (*key)[i] = byte + 1; // NOTE: htt, 非0xff的字节，自增
        key->resize(i+1);
        return;
      }
    }
    // *key is a run of 0xffs.  Leave it alone.
  }
};
}  // namespace

static port::OnceType once = LEVELDB_ONCE_INIT;
static const Comparator* bytewise;

static void InitModule() {
  bytewise = new BytewiseComparatorImpl; // NOTE: htt, 本文件函数，设置comparator为 BytewiseComparatorImpl
}

const Comparator* BytewiseComparator() {
  port::InitOnce(&once, InitModule); // NOTE: htt, 执行初始化
  return bytewise; // NOTE: htt, 返回私有全局变量 bytewise
}

}  // namespace leveldb
