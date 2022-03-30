// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_H_

#include <stddef.h>
#include <stdint.h>
#include "leveldb/iterator.h"

namespace leveldb {

struct BlockContents; // NOTE: htt, 读取到内存block块内容
class Comparator;

class Block { // NOTE: htt, block块,包括<key,value>，采用restart数组，实现二分查找
 public:
  // Initialize the block with the specified contents.
  explicit Block(const BlockContents& contents);

  ~Block();

  size_t size() const { return size_; }
  Iterator* NewIterator(const Comparator* comparator);

 private:
  uint32_t NumRestarts() const;

  const char* data_; // NOTE: htt, 整个block数据块的指针位置
  size_t size_; // NOTE: htt, 整个block块长度 
  uint32_t restart_offset_;     // Offset in data_ of restart array // NOTE: htt, restart数组的起始偏移
  bool owned_;                  // Block owns data_[] //NOTE: htt, 数据块block是否为单独 new buf[]对象

  // No copying allowed
  Block(const Block&);
  void operator=(const Block&);

  class Iter;// NOTE: htt, block内部迭代器,实现根据restart[]索引找到record位置,然后递增entry获取下一个record
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_H_
