// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include "table/block_builder.h"

#include <assert.h>
#include <algorithm>
#include "leveldb/comparator.h"
#include "leveldb/table_builder.h"
#include "util/coding.h"

namespace leveldb {

BlockBuilder::BlockBuilder(const Options* options) : options_(options), restarts_(), counter_(0), finished_(false) {
  assert(options->block_restart_interval >= 1);
  restarts_.push_back(0);  // First restart point is at offset 0
}

void BlockBuilder::Reset() {  // NOTE: htt, 重置
  buffer_.clear();
  restarts_.clear();
  restarts_.push_back(0);  // First restart point is at offset 0 // NOTE: htt,第一个restart[]对应record偏移为0
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
}

size_t BlockBuilder::CurrentSizeEstimate() const {  // NOTE: htt, 评估block大小,包括 records列表+restart数组+num restart
  return (buffer_.size() +                          // Raw data buffer
          restarts_.size() * sizeof(uint32_t) +  // Restart array
          sizeof(uint32_t));                     // Restart array length
}

Slice BlockBuilder::Finish() {  // NOTE: htt, 完成block的生成
  // Append restart array
  for (size_t i = 0; i < restarts_.size(); i++) {
    PutFixed32(&buffer_, restarts_[i]);  // NOTE: htt, 将restart[]数组添加到block中
  }
  PutFixed32(&buffer_, restarts_.size());  // NOTE: htt, 添加restart[]数组的长度
  finished_ = true;                        // NOTE: htt, block已生成完成
  return Slice(buffer_);
}

void BlockBuilder::Add(const Slice& key,
                       const Slice& value) {  // NOTE: htt, 在block中添加<key,value>,并根据需要生成新restart
  Slice last_key_piece(last_key_);
  assert(!finished_);
  assert(counter_ <= options_->block_restart_interval);
  assert(buffer_.empty()                                              // No values yet?
         || options_->comparator->Compare(key, last_key_piece) > 0);  // NOTE: htt, 保证key有序
  size_t shared = 0;
  if (counter_ < options_->block_restart_interval) {
    // See how much sharing to do with previous string
    const size_t min_length = std::min(last_key_piece.size(), key.size());
    while ((shared < min_length) &&
           (last_key_piece[shared] == key[shared])) {  // NOTE: htt, 找到当前key和已存储key最大shared长度
      shared++;
    }
  } else {  // NOTE: htt, 如果达到restart对应records个数，则生成新的restart;
            // 此时shared为0,即key共享长度从restart开始
    // Restart compression
    restarts_.push_back(buffer_.size());  // NOTE: htt, 新restart对应records的偏移
    counter_ = 0;                         // NOTE: htt, 新restart下records个数从0计算
  }
  const size_t non_shared = key.size() - shared;

  // Add "<shared><non_shared><value_size>" to buffer_
  PutVarint32(&buffer_, shared);        // NOTE: htt, 保存共享长度
  PutVarint32(&buffer_, non_shared);    // NOTE: htt, 保存当前key非共享长度
  PutVarint32(&buffer_, value.size());  // NOTE: htt, 保存value长度

  // Add string delta to buffer_ followed by value
  buffer_.append(key.data() + shared, non_shared);  // NOTE: htt, 保存key的非共享内容
  buffer_.append(value.data(), value.size());       // NOTE: htt, 保存value内容

  // Update state
  last_key_.resize(shared);                           // NOTE: htt, last_key留下共享长度内容
  last_key_.append(key.data() + shared, non_shared);  // NOTE: htt, last_key添加新key的非共享内容
  assert(Slice(last_key_) == key);
  counter_++;  // NOTE: htt, 增加当前restart对应的records个数
}

}  // namespace leveldb
