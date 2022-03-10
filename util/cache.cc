// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "leveldb/cache.h"
#include "port/port.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {

Cache::~Cache() {
}

namespace {

// LRU cache implementation

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
struct LRUHandle { // NOTE: htt, Handle内部一个是吸纳LRUHandle
  void* value;
  void (*deleter)(const Slice&, void* value);
  LRUHandle* next_hash; // NOTE: htt, hashTable中 hash后处于同一个bucket中的下一个Handle的指针
  LRUHandle* next;
  LRUHandle* prev;
  size_t charge;      // TODO(opt): Only allow uint32_t? // NOTE: htt, 消耗空间
  size_t key_length; // NOTE: htt, key的长度
  uint32_t refs; // NOTE: htt, 当前对象被引用的次数
  uint32_t hash;      // Hash of key(); used for fast sharding and comparisons // NOTE: htt, 当前key的hash值
  char key_data[1];   // Beginning of key // NOTE: htt, key开始的字节，后续空间是创建时按需分配

  Slice key() const { // NOTE: htt, 获取LRUHandle的key
    // For cheaper lookups, we allow a temporary Handle object
    // to store a pointer to a key in "value".
    if (next == this) {
      return *(reinterpret_cast<Slice*>(value));
    } else {
      return Slice(key_data, key_length);
    }
  }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class HandleTable { // NOTE: htt, Handle的HashTable,支持查找/插入/删除 /*{{{*/
 public:
  HandleTable() : length_(0), elems_(0), list_(NULL) { Resize(); }
  ~HandleTable() { delete[] list_; } // NOTE: htt, 删除list_

  LRUHandle* Lookup(const Slice& key, uint32_t hash) { // NOTE: htt, 查找 key(包括hash值)
    return *FindPointer(key, hash);
  }

  LRUHandle* Insert(LRUHandle* h) { // NOTE: htt, 插入LRUHandle /*{{{*/
    LRUHandle** ptr = FindPointer(h->key(), h->hash);
    LRUHandle* old = *ptr;
    h->next_hash = (old == NULL ? NULL : old->next_hash); // NOTE: htt, 替换现有相同个key
    *ptr = h;
    if (old == NULL) {
      ++elems_; // NOTE: htt, 增加 elems_ 值
      if (elems_ > length_) { // NOTE: htt, hashTalbe中 元素个数 大于 hashTalbe的长度，进行Resize()
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    }
    return old; // NOTE: htt, 返回替换的LRUHandle
  }/*}}}*/

  LRUHandle* Remove(const Slice& key, uint32_t hash) { // NOTE: htt, 移除HashTable中key /*{{{*/
    LRUHandle** ptr = FindPointer(key, hash);
    LRUHandle* result = *ptr;
    if (result != NULL) {
      *ptr = result->next_hash;
      --elems_; // NOTE: htt, 减少 elems_ 值
    }
    return result; // NOTE: htt, 返回移除的 LRUHandle
  }/*}}}*/

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_; // NOTE: htt, table的长度, 4的整数倍
  uint32_t elems_; // NOTE: htt, table中的元素个数
  LRUHandle** list_; // NOTE: htt, hashTable列表

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {/*{{{*/
    LRUHandle** ptr = &list_[hash & (length_ - 1)];
    while (*ptr != NULL &&
           ((*ptr)->hash != hash || key != (*ptr)->key())) { // NOTE: htt, 遍历当前 list_[hash]桶下的 LRUHandle列表
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }/*}}}*/

  void Resize() { // NOTE: htt, 重建hashTable /*{{{*/
    uint32_t new_length = 4;
    while (new_length < elems_) {
      new_length *= 2; // NOTE: htt, 按2的倍数递增
    }
    LRUHandle** new_list = new LRUHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) { // NOTE: htt, 重建已有的 LRUHandle table
      LRUHandle* h = list_[i];
      while (h != NULL) { // NOTE: htt, 重建某个hash值下的 LRUHandle
        LRUHandle* next = h->next_hash;
        uint32_t hash = h->hash;
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];
        h->next_hash = *ptr;
        *ptr = h;
        h = next;
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_; // NOTE: htt, 删除已有HashTable链
    list_ = new_list;
    length_ = new_length; // NOTE: htt, 新的hashTable长度
  }/*}}}*/
};/*}}}*/

// A single shard of sharded cache.
class LRUCache {/*{{{*/
 public:
  LRUCache();
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  // Like Cache methods, but with an extra "hash" parameter.
  Cache::Handle* Insert(const Slice& key, uint32_t hash,
                        void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value));
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle* e);
  void Unref(LRUHandle* e);

  // Initialized before use.
  size_t capacity_; // NOTE: htt, 支持的容量

  // mutex_ protects the following state.
  port::Mutex mutex_; // NOTE: htt, 互斥锁
  size_t usage_; // NOTE: htt, 已使用的空间

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  LRUHandle lru_; // NOTE: htt, LRU list的头部, LRU机制是头部为旧，尾部为新，LRU移除时，先从头部开始移除

  HandleTable table_; // NOTE: htt, HashTable
};/*}}}*/

LRUCache::LRUCache()
    : usage_(0) {/*{{{*/
  // Make empty circular linked list
  lru_.next = &lru_;
  lru_.prev = &lru_;
}/*}}}*/

LRUCache::~LRUCache() {/*{{{*/
  for (LRUHandle* e = lru_.next; e != &lru_; ) { // NOTE: htt, 是否LRUHandle中的所有对象
    LRUHandle* next = e->next;
    assert(e->refs == 1);  // Error if caller has an unreleased handle
    Unref(e); // NOTE: htt, 减少LRUHandle对应引用
    e = next;
  }
}/*}}}*/

void LRUCache::Unref(LRUHandle* e) {// NOTE: htt, 减少引用，并为0时需要释放 /*{{{*/
  assert(e->refs > 0);
  e->refs--;
  if (e->refs <= 0) { // NOTE: htt, LRUHandle 引用为0则删除对象
    usage_ -= e->charge; // NOTE: htt, 减少空间占用
    (*e->deleter)(e->key(), e->value); // NOTE: htt, 删除LRUHandle中的 <key,value>
    free(e); // NOTE: htt, 释放e
  }
}/*}}}*/

void LRUCache::LRU_Remove(LRUHandle* e) { // NOTE: htt, 移除LRUHandle在链中位置 /*{{{*/
  e->next->prev = e->prev;
  e->prev->next = e->next;
}/*}}}*/

void LRUCache::LRU_Append(LRUHandle* e) { // NOTE: htt, 尾部追加LRUHandle /*{{{*/
  // Make "e" newest entry by inserting just before lru_
  e->next = &lru_;
  e->prev = lru_.prev;
  e->prev->next = e;
  e->next->prev = e;
}/*}}}*/

Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) { // NOTE: htt, 查找key, 如果找到则增加引用并调整LRU位置/*{{{*/
  MutexLock l(&mutex_); // NOTE: htt, 加锁处理
  LRUHandle* e = table_.Lookup(key, hash); // NOTE: htt, table_中快速查找
  if (e != NULL) {
    e->refs++; // NOTE: htt, 增加引用次数
    LRU_Remove(e); // NOTE: htt, 删除链表位置
    LRU_Append(e); // NOTE: htt, 追加到尾部
  }
  return reinterpret_cast<Cache::Handle*>(e);
}/*}}}*/

void LRUCache::Release(Cache::Handle* handle) { // NOTE: htt, 是否对象,根据引用判断是否需要销毁
  MutexLock l(&mutex_); // NOTE: htt, 加锁处理
  Unref(reinterpret_cast<LRUHandle*>(handle)); // NOTE: htt, 减少LRUHandle引用
}

Cache::Handle* LRUCache::Insert(
    const Slice& key, uint32_t hash, void* value, size_t charge,
    void (*deleter)(const Slice& key, void* value)) {// NOTE: htt, 插入对象，并判断是否LRU，如果是则LRU淘汰旧数据/*{{{*/
  MutexLock l(&mutex_); // NOTE: htt, 加锁

  LRUHandle* e = reinterpret_cast<LRUHandle*>(
      malloc(sizeof(LRUHandle)-1 + key.size())); // NOTE: htt, 创建实际长度的LRUHandle
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->refs = 2;  // One from LRUCache, one for the returned handle
  memcpy(e->key_data, key.data(), key.size()); // NOTE: htt, 复制值
  LRU_Append(e); // NOTE: htt, LRU链中尾部追加
  usage_ += charge;

  LRUHandle* old = table_.Insert(e);
  if (old != NULL) { // NOTE: htt, 如果存在旧对象则进行删除
    LRU_Remove(old); // NOTE: htt, LRU链中去除
    Unref(old); // NOTE: htt, 减少引用，如果为0则释放对象
  }

  while (usage_ > capacity_ && lru_.next != &lru_) { // NOTE: htt, 如果超过容量限制，则LRU机制移除对象
    LRUHandle* old = lru_.next; // NOTE: htt, 从lru头部开始删除对象
    LRU_Remove(old); // NOTE: htt, LRU链中移除
    table_.Remove(old->key(), old->hash); // NOTE: htt, HashTable中移除
    Unref(old); // NOTE: htt, 减少引用，如果为0则释放对象
  }

  return reinterpret_cast<Cache::Handle*>(e);
}/*}}}*/

void LRUCache::Erase(const Slice& key, uint32_t hash) { // NOTE: htt, 释放key对象 /*{{{*/
  MutexLock l(&mutex_); // NOTE: htt, 加锁
  LRUHandle* e = table_.Remove(key, hash); // NOTE: htt, hashTable中移除对象
  if (e != NULL) { // NOTE: htt, 对象不为空则释放
    LRU_Remove(e); // NOTE: htt, LRU链中移除
    Unref(e); // NOTE: htt, 减少引用，如果为0则释放对象
  }
}/*}}}*/

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

class ShardedLRUCache : public Cache { // NOTE: htt, 带分片的LRUCache, 即两次hash机制 /*{{{*/
 private:
  LRUCache shard_[kNumShards]; // NOTE: htt, 16个 LRUCache对象, 即两次hash减少碰撞
  port::Mutex id_mutex_; // NOTE: htt, id锁
  uint64_t last_id_;

  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0); // NOTE: htt, 返回hash值
  }

  static uint32_t Shard(uint32_t hash) { // NOTE: htt, 确认当前hash在哪个LRUCache上(也可以认为在哪个分片上)
    return hash >> (32 - kNumShardBits);
  }

 public:
  explicit ShardedLRUCache(size_t capacity)
      : last_id_(0) {
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards; // NOTE: htt, 4的整数倍
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard); // NOTE: htt, 设置每个shard的capacity容量
    }
  }
  virtual ~ShardedLRUCache() { }
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) { // NOTE: htt, 插入<key,value>
    const uint32_t hash = HashSlice(key); // NOTE: htt, 获取key的hash值
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter); // NOTE: htt, 在对应LRUCache shard分片上插入<key,value>
  }
  virtual Handle* Lookup(const Slice& key) { // NOTE: htt, 查找key
    const uint32_t hash = HashSlice(key); // NOTE: htt, 获取key的hash值
    return shard_[Shard(hash)].Lookup(key, hash); // NOTE: htt, 在LRUCache shard分片上查找key
  }
  virtual void Release(Handle* handle) {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle); // NOTE: htt, 在对应shard分片上是否Handle对象
  }
  virtual void Erase(const Slice& key) {
    const uint32_t hash = HashSlice(key); // NOTE: htt, 获取key的hash值
    shard_[Shard(hash)].Erase(key, hash); // NOTE: htt, 在LRUHandle shard分片上释放key
  }
  virtual void* Value(Handle* handle) { // NOTE: htt, 获取handle的value
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  virtual uint64_t NewId() {
    MutexLock l(&id_mutex_); // NOTE: htt, 获取id锁
    return ++(last_id_); // NOTE: htt, 自增id
  }
};/*}}}*/

}  // end anonymous namespace

Cache* NewLRUCache(size_t capacity) { // NOTE: htt, 获得ShardedLRUCache
  return new ShardedLRUCache(capacity);
}

}  // namespace leveldb
