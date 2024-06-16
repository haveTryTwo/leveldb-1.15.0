// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_ARENA_H_
#define STORAGE_LEVELDB_UTIL_ARENA_H_

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <vector>

namespace leveldb {

class Arena {  // NOTE:htt, 提供内存分配机制,如果小内存则先缓存一部分,后续申请直接从缓存取
 public:
  Arena();
  ~Arena();

  // Return a pointer to a newly allocated memory block of "bytes" bytes.
  char* Allocate(size_t bytes);  // NOTE: htt, 分配内存

  // Allocate memory with the normal alignment guarantees provided by malloc
  char* AllocateAligned(size_t bytes);  // NOTE: htt, 对齐分配内存

  // Returns an estimate of the total memory usage of data allocated
  // by the arena (including space allocated but not yet used for user
  // allocations).
  size_t MemoryUsage() const { return blocks_memory_ + blocks_.capacity() * sizeof(char*); }

 private:
  char* AllocateFallback(size_t bytes);
  char* AllocateNewBlock(size_t block_bytes);

  // Allocation state
  char* alloc_ptr_;               // NOTE: htt, 空闲内存的起始位置
  size_t alloc_bytes_remaining_;  // NOTE: htt, 当前有的剩余空闲内存

  // Array of new[] allocated memory blocks
  std::vector<char*> blocks_;  // NOTE: htt, 已分配的内存

  // Bytes of memory in blocks allocated so far
  size_t blocks_memory_;  // NOTE: htt, 已经分配的内存

  // No copying allowed
  Arena(const Arena&);
  void operator=(const Arena&);
};

inline char* Arena::Allocate(size_t bytes) {
  // The semantics of what to return are a bit messy if we allow
  // 0-byte allocations, so we disallow them here (we don't need
  // them for our internal use).
  assert(bytes > 0);
  if (bytes <= alloc_bytes_remaining_) {
    char* result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
  }
  return AllocateFallback(bytes);
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_ARENA_H_
