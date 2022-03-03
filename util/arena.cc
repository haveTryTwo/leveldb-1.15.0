// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"
#include <assert.h>

namespace leveldb {

static const int kBlockSize = 4096;

Arena::Arena() {
  blocks_memory_ = 0;
  alloc_ptr_ = NULL;  // First allocation will allocate a block
  alloc_bytes_remaining_ = 0;
}

Arena::~Arena() {
  for (size_t i = 0; i < blocks_.size(); i++) {
    delete[] blocks_[i]; // NOTE: htt, 删除已分配内存
  }
}

char* Arena::AllocateFallback(size_t bytes) { // NOTE: htt, 重新申请内存，并分配
  if (bytes > kBlockSize / 4) { // NOTE: htt, 大于 kBlockSize/4, 则直接分配新的内存
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    char* result = AllocateNewBlock(bytes);
    return result;
  }

  // We waste the remaining space in the current block.
  alloc_ptr_ = AllocateNewBlock(kBlockSize); // NOTE: htt, 如果<= kBlockSize/4，则先分配 kBlockSize 大小内存
  alloc_bytes_remaining_ = kBlockSize; // NOTE: htt, 空闲内存

  char* result = alloc_ptr_;
  alloc_ptr_ += bytes; // NOTE: htt, 调整空闲内存地址
  alloc_bytes_remaining_ -= bytes; // NOTE: htt, 调整剩余空闲内存
  return result;
}

char* Arena::AllocateAligned(size_t bytes) { // NOTE: htt, 按8长度对齐地址
  const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
  assert((align & (align-1)) == 0);   // Pointer size should be a power of 2
  size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align-1);
  size_t slop = (current_mod == 0 ? 0 : align - current_mod); // NOTE: htt, 对齐需要补齐长度
  size_t needed = bytes + slop; // NOTE: htt, 为了对齐，需要将bytes+slop(即补齐长度)
  char* result;
  if (needed <= alloc_bytes_remaining_) {
    result = alloc_ptr_ + slop; // NOTE: htt, 调整到补齐的位置
    alloc_ptr_ += needed; // NOTE: htt, 调整空闲地址，需要为 bytes+补齐空间长度
    alloc_bytes_remaining_ -= needed; // NOTE: htt, 调整空闲长度
  } else {
    // AllocateFallback always returned aligned memory
    result = AllocateFallback(bytes); // NOTE: htt, 重新升起一定为对齐地址
  }
  assert((reinterpret_cast<uintptr_t>(result) & (align-1)) == 0); // NOTE: htt, 确认地址为补齐地址
  return result;
}

char* Arena::AllocateNewBlock(size_t block_bytes) { // NOTE: htt, 分配新的内存
  char* result = new char[block_bytes];
  blocks_memory_ += block_bytes;
  blocks_.push_back(result); // NOTE: htt, 新申请的空间即添加到blocks_列表中,用于后续回收
  return result;
}

}  // namespace leveldb
