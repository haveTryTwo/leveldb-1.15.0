// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_

#include <vector>

#include <stdint.h>
#include "leveldb/slice.h"

namespace leveldb {

struct Options;

class BlockBuilder { // NOTE: htt, 生成block, 不断添加<key,value>,并根据需要调整restart[]数组偏移
 public:
  explicit BlockBuilder(const Options* options);

  // Reset the contents as if the BlockBuilder was just constructed.
  void Reset();

  // REQUIRES: Finish() has not been callled since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  void Add(const Slice& key, const Slice& value);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  Slice Finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  size_t CurrentSizeEstimate() const;

  // Return true iff no entries have been added since the last Reset()
  bool empty() const {
    return buffer_.empty();
  }

 private:
  const Options*        options_;
  std::string           buffer_;      // Destination buffer // NOTE: htt, 保存block内容的buffer
  std::vector<uint32_t> restarts_;    // Restart points // NOTE: htt, restart[]保存的records的偏移
  int                   counter_;     // Number of entries emitted since restart // NOTE: htt, 从restart开始对应records个数
  bool                  finished_;    // Has Finish() been called? // NOTE: htt, block是否已完成
  std::string           last_key_;    // NOTE: htt, 添加到buffer中的最后一个key,用于实现和新增key的共享长度判断

  // No copying allowed
  BlockBuilder(const BlockBuilder&);
  void operator=(const BlockBuilder&);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
