// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.

#ifndef STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>
#include "leveldb/slice.h"
#include "util/hash.h"

namespace leveldb {

class FilterPolicy;

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish
class FilterBlockBuilder { // NOTE:htt,构建block的bloomFilter;针对block先添加key列表,然后生成bloomFilter放入result中
 public:
  explicit FilterBlockBuilder(const FilterPolicy*);

  void StartBlock(uint64_t block_offset);
  void AddKey(const Slice& key);
  Slice Finish();

 private:
  void GenerateFilter();

  const FilterPolicy* policy_; // NOTE:htt, 过滤策略
  std::string keys_;              // Flattened key contents // NOTE:htt, 包含key的列表
  std::vector<size_t> start_;     // Starting index in keys_ of each key // NOTE:htt, key在keys中的偏移
  std::string result_;            // Filter data computed so far //NOTE:htt,保存bloomFilter结果列表,按block分成多个偏移
  std::vector<Slice> tmp_keys_;   // policy_->CreateFilter() argument // NOTE:htt, 临时保存key的列表
  std::vector<uint32_t> filter_offsets_; // NOTE: htt,保存result中按block分成的多个块的开始偏移

  // No copying allowed
  FilterBlockBuilder(const FilterBlockBuilder&);
  void operator=(const FilterBlockBuilder&);
};

class FilterBlockReader { // NOTE:htt, 从已构建的bloomFilter解析列表,然后对相应的key进行bloomFilter查询
 public:
 // REQUIRES: "contents" and *policy must stay live while *this is live.
  FilterBlockReader(const FilterPolicy* policy, const Slice& contents);
  bool KeyMayMatch(uint64_t block_offset, const Slice& key);

 private:
  const FilterPolicy* policy_;
  const char* data_;    // Pointer to filter data (at block-start) // NOTE:htt, block对应bloomFilter的起始位置
  const char* offset_;  // Pointer to beginning of offset array (at block-end) // NOTE:htt,首个block对应bloomFilter偏移在data偏移
  size_t num_;          // Number of entries in offset array // NOTE:htt, block对应bloomFilter的偏移列表个数
  size_t base_lg_;      // Encoding parameter (see kFilterBaseLg in .cc file) // NOTE:htt,bloomFilter对应分块大小,默认2k
};

}

#endif  // STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
