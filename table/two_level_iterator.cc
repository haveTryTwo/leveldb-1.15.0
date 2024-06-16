// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"

#include "leveldb/table.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {

typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&);

class TwoLevelIterator : public Iterator {  // NOTE:htt, 包含index迭代器和data迭代器的双层迭代器
 public:
  TwoLevelIterator(Iterator* index_iter, BlockFunction block_function, void* arg, const ReadOptions& options);

  virtual ~TwoLevelIterator();

  virtual void Seek(const Slice& target);
  virtual void SeekToFirst();
  virtual void SeekToLast();
  virtual void Next();
  virtual void Prev();

  virtual bool Valid() const {  // NOTE:htt, data迭代器是否有效
    return data_iter_.Valid();
  }
  virtual Slice key() const {  // NOTE:htt, 获取数据key
    assert(Valid());
    return data_iter_.key();
  }
  virtual Slice value() const {  // NOTE:htt, 获取数据value
    assert(Valid());
    return data_iter_.value();
  }
  virtual Status status() const {  // NOTE:htt, 返回当前的状态
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!index_iter_.status().ok()) {
      return index_iter_.status();
    } else if (data_iter_.iter() != NULL && !data_iter_.status().ok()) {
      return data_iter_.status();
    } else {
      return status_;  // NOTE:htt, 如果index iter 和 data iter 都正常情况,则返回当前status_
    }
  }

 private:
  void SaveError(const Status& s) {  // NOTE:htt, 设置error状态
    if (status_.ok() && !s.ok()) status_ = s;
  }
  void SkipEmptyDataBlocksForward();
  void SkipEmptyDataBlocksBackward();
  void SetDataIterator(Iterator* data_iter);
  void InitDataBlock();

  BlockFunction block_function_;  // NOTE:htt, 通过index迭代器获取内容构建data block的迭代器,
                                  // Table:BlockReader()
  void* arg_;                     // NOTE:htt, 参数,构建data迭代器, 为Table
  const ReadOptions options_;     // NOTE:htt, 读options
  Status status_;
  IteratorWrapper index_iter_;  // NOTE:htt, index迭代器,value指向blocks块
  IteratorWrapper data_iter_;   // May be NULL // NOTE:htt, data迭代器,为实际的 <key,value>
  // If data_iter_ is non-NULL, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the data_iter_.
  std::string data_block_handle_;  // NOTE:htt, data block块的内容引用
};

TwoLevelIterator::TwoLevelIterator(Iterator* index_iter, BlockFunction block_function, void* arg,
                                   const ReadOptions& options)
    : block_function_(block_function), arg_(arg), options_(options), index_iter_(index_iter), data_iter_(NULL) {}

TwoLevelIterator::~TwoLevelIterator() {}

void TwoLevelIterator::Seek(const Slice& target) {         // NOTE:htt, 查找key
  index_iter_.Seek(target);                                // NOTE:htt, index迭代器找到key
  InitDataBlock();                                         // NOTE:htt, 初始化data迭代器
  if (data_iter_.iter() != NULL) data_iter_.Seek(target);  // NOTE:htt, data迭代器找到内容
  SkipEmptyDataBlocksForward();                            // NOTE:htt, 向前找第一个有效data迭代器
}

void TwoLevelIterator::SeekToFirst() {                      // NOTE:htt, data迭代器指向第一个<key,value>
  index_iter_.SeekToFirst();                                // NOTE:htt, index迭代器指向第一个
  InitDataBlock();                                          // NOTE:htt, 初始化data迭代器
  if (data_iter_.iter() != NULL) data_iter_.SeekToFirst();  // NOTE:htt, data迭代器指向第一个内容
  SkipEmptyDataBlocksForward();                             // NOTE:htt, 向前找第一个有效data迭代器
}

void TwoLevelIterator::SeekToLast() {  // NOTE:htt,
  index_iter_.SeekToLast();            // NOTE:htt, index迭代器指向最后一个<key,value>
  InitDataBlock();
  if (data_iter_.iter() != NULL) data_iter_.SeekToLast();  // NOTE:htt, data迭代器指向最后一个<key,value>
  SkipEmptyDataBlocksBackward();                           // NOTE:htt, 向后查找最后一个有效data迭代器
}

void TwoLevelIterator::Next() {  // NOTE:htt, data迭代器向下移动
  assert(Valid());
  data_iter_.Next();             // NOTE:htt, data迭代器指向下一个
  SkipEmptyDataBlocksForward();  // NOTE:htt, 向前找第一个有效data迭代器
}

void TwoLevelIterator::Prev() {  // NOTE:htt, data迭代器向上移动
  assert(Valid());
  data_iter_.Prev();              // NOTE:htt, data迭代器指向上一个
  SkipEmptyDataBlocksBackward();  // NOTE:htt, 向后查找最后一个有效data迭代器
}

void TwoLevelIterator::SkipEmptyDataBlocksForward() {  // NOTE:htt, 向前查找第一个有效data迭代器
  while (data_iter_.iter() == NULL ||
         !data_iter_.Valid()) {  // NOTE:htt, 如果data迭代器无效则尝试继续查找,生成有效迭代器
    // Move to next block
    if (!index_iter_.Valid()) {  // NOTE:htt,如果index迭代器无效则设置data迭代器为NULL
      SetDataIterator(NULL);
      return;
    }
    index_iter_.Next();  // NOTE:htt, index迭代器指向下一个
    InitDataBlock();
    if (data_iter_.iter() != NULL) data_iter_.SeekToFirst();  // NOTE:htt, data迭代器指向第一个
  }
}

void TwoLevelIterator::SkipEmptyDataBlocksBackward() {  // NOTE:htt, 向后查找最后一个有效data迭代器
  while (data_iter_.iter() == NULL ||
         !data_iter_.Valid()) {  // NOTE:htt, 如果data迭代器无效则尝试继续查找,生成有效迭代器
    // Move to next block
    if (!index_iter_.Valid()) {  // NOTE:htt,如果index迭代器无效则设置data迭代器为NULL
      SetDataIterator(NULL);
      return;
    }
    index_iter_.Prev();  // NOTE:htt, index迭代器指向前一个
    InitDataBlock();
    if (data_iter_.iter() != NULL) data_iter_.SeekToLast();  // NOTE:htt, data迭代器指向最后一个<key,value>
  }
}

void TwoLevelIterator::SetDataIterator(Iterator* data_iter) {     // NOTE:htt, 设置 data迭代器
  if (data_iter_.iter() != NULL) SaveError(data_iter_.status());  // NOTE:htt, data_iter_ 非NULL则设置其中状态
  data_iter_.Set(data_iter);                                      // NOTE:htt, 设置 data 迭代器
}

void TwoLevelIterator::InitDataBlock() {  // NOTE:htt, 初始化data block的data迭代器
  if (!index_iter_.Valid()) {             // NOTE:htt, 如果index迭代器异常,则设置data迭代器为NULL
    SetDataIterator(NULL);
  } else {
    Slice handle = index_iter_.value();
    if (data_iter_.iter() != NULL && handle.compare(data_block_handle_) == 0) {  // NOTE:htt, data内容相同则不用设置
      // data_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {
      Iterator* iter = (*block_function_)(arg_, options_, handle);  // NOTE:htt, 生成data迭代器
      data_block_handle_.assign(handle.data(), handle.size());  // NOTE:htt, data_block_handle_保存data block起始信息
      SetDataIterator(iter);                                    // NOTE:htt, 设置data迭代器
    }
  }
}

}  // namespace

Iterator* NewTwoLevelIterator(Iterator* index_iter, BlockFunction block_function, void* arg,
                              const ReadOptions& options) {  // NOTE:htt, 构建data迭代器
  return new TwoLevelIterator(index_iter, block_function, arg, options);
}

}  // namespace leveldb
