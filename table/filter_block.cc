// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.txt for an explanation of the filter block format.

// Generate new filter every 2KB of data
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy) : policy_(policy) {}

void FilterBlockBuilder::StartBlock(
    uint64_t block_offset) {  // NOTE:htt, key添加完毕后,生成对应block的bloomFilter并添加result中
  uint64_t filter_index = (block_offset / kFilterBase);  // NOTE:htt, block大小超过2K
  assert(filter_index >= filter_offsets_.size());
  while (filter_index >
         filter_offsets_.size()) {  // NOTE:htt,
                                    // 按2K生成一个Filter,若keys为空,则按2K在filter_offsets补results的偏移
    GenerateFilter();
  }
}

void FilterBlockBuilder::AddKey(
    const Slice& key) {  // NOTE:htt, 添加一个key,对应在一个block内,超过一个block则生成bloom filter
  Slice k = key;
  start_.push_back(keys_.size());    // NOTE:htt, 记录新key在keys中的偏移
  keys_.append(k.data(), k.size());  // NOTE:htt, 包括keys到字符串中
}

Slice FilterBlockBuilder::Finish() {  // NOTE:htt,
                                      // 将block对应的bloomFilter以及每个bloomFilter偏移添加到result字符串内并返回
  if (!start_.empty()) {
    GenerateFilter();  // NOTE: htt, 将一批key生成bloomFilter,保存到result
  }

  // Append array of per-filter offsets
  const uint32_t array_offset = result_.size();
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_,
               filter_offsets_[i]);  // NOTE: htt,将每个block的bloomFilter偏移追加到result中
  }

  PutFixed32(&result_,
             array_offset);          // NOTE:htt, 追加首个block对应bloomFilter的起始偏移位置到result中
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result // NOTE:htt,
                                     // 追加bloomFilter分块大小,一个字节
  return Slice(result_);
}

void FilterBlockBuilder::GenerateFilter() {  // NOTE: htt,
                                             // 将一批key生成bloomFilter,保存到result,由filter
                                             // offsets记录每个偏移
  const size_t num_keys = start_.size();     // NOTE:htt, key的个数为0
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  start_.push_back(keys_.size());  // Simplify length computation // NOTE:htt,添加末尾偏移
  tmp_keys_.resize(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    const char* base = keys_.data() + start_[i];  // NOTE:htt, 某个key起始位置
    size_t length = start_[i + 1] - start_[i];    // NOTE:htt, 某个key的长度
    tmp_keys_[i] = Slice(base, length);           // NOTE:htt, 临时保存key
  }

  // Generate filter for current set of keys and append to result_.
  filter_offsets_.push_back(result_.size());  // NOTE:htt, 记录当前的偏移
  policy_->CreateFilter(&tmp_keys_[0], num_keys,
                        &result_);  // NOTE: htt, 新的一批key生成新的bloomFilter,并保存到result

  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

FilterBlockReader::FilterBlockReader(const FilterPolicy* policy, const Slice& contents)
    : policy_(policy), data_(NULL), offset_(NULL), num_(0), base_lg_(0) {
  size_t n = contents.size();
  if (n < 5) return;                                            // 1 byte for base_lg_ and 4 for start of offset array
  base_lg_ = contents[n - 1];                                   // NOTE:htt,获得分块的大小
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5);  // NOTE:htt,首个block对应bloomFilter偏移在result中偏移
  if (last_word > n - 5) return;
  data_ = contents.data();
  offset_ = data_ + last_word;
  num_ = (n - 5 - last_word) / 4;  // NOTE:htt,获取block对应bloomFilter偏移的大小 / 4,即对应个数
}

bool FilterBlockReader::KeyMayMatch(
    uint64_t block_offset,
    const Slice& key) {  // NOTE:htt,判断可以是否存在,先找到偏移,再读取bloomFilter,再进行判断
  uint64_t index = block_offset >> base_lg_;
  if (index < num_) {
    uint32_t start = DecodeFixed32(offset_ + index * 4);  // NOTE:htt, 找到block offset对应的bloomFilter偏移起始位置
    uint32_t limit = DecodeFixed32(offset_ + index * 4 +
                                   4);  // NOTE:htt, block offset对应bloomFilter的结束位置(即下一个block起始位置)
    if (start <= limit && limit <= (offset_ - data_)) {
      Slice filter = Slice(data_ + start, limit - start);
      return policy_->KeyMayMatch(key, filter);  // NOTE:htt, 通过bloomFilter判断key是否存在
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}  // namespace leveldb
