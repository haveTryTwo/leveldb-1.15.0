// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/filter_policy.h"

#include "leveldb/slice.h"
#include "util/hash.h"

namespace leveldb {

namespace {
static uint32_t BloomHash(const Slice& key) { // NOTE: htt, 采用bloom hash当前的字符串
  return Hash(key.data(), key.size(), 0xbc9f1d34); // NOTE: htt, bloom hash当前的字符串
}

class BloomFilterPolicy : public FilterPolicy { // NOTE: htt, bloom过滤器策略,包括生成新的bloom值，和检查值在不在bloom中
 private:
  size_t bits_per_key_; // NOTE: htt, 每个key占用的bits，推荐为10
  size_t k_; // NOTE: htt, hash的次数，推荐为3，即判断需要进行3次

 public:
  explicit BloomFilterPolicy(int bits_per_key)
      : bits_per_key_(bits_per_key) {
    // We intentionally round down to reduce probing cost a little bit
    k_ = static_cast<size_t>(bits_per_key * 0.69);  // 0.69 =~ ln(2)
    if (k_ < 1) k_ = 1;
    if (k_ > 30) k_ = 30;
  }

  virtual const char* Name() const {
    return "leveldb.BuiltinBloomFilter"; // NOTE: htt, 过滤器名称
  }

  virtual void CreateFilter(const Slice* keys, int n, std::string* dst) const { // NOTE: htt, 创建一批keys的bloom值，并追加到dst
    // Compute bloom filter size (in both bits and bytes)
    size_t bits = n * bits_per_key_;

    // For small n, we can see a very high false positive rate.  Fix it
    // by enforcing a minimum bloom filter length.
    if (bits < 64) bits = 64; // NOTE: htt, bloom数组最少64bit

    size_t bytes = (bits + 7) / 8;
    bits = bytes * 8; // NOTE: htt, 数组为8的整数倍

    const size_t init_size = dst->size();
    dst->resize(init_size + bytes, 0); // NOTE: htt, 追加dst数组
    dst->push_back(static_cast<char>(k_));  // Remember # of probes in filter, // NOTE: htt, 最后一字节追加 k_
    char* array = &(*dst)[init_size];
    for (size_t i = 0; i < n; i++) {
      // Use double-hashing to generate a sequence of hash values.
      // See analysis in [Kirsch,Mitzenmacher 2006].
      uint32_t h = BloomHash(keys[i]);
      const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
      for (size_t j = 0; j < k_; j++) { // NOTE: htt, hash 1次，然后采用 h+delta方式增加，作为 k_次的hash
        const uint32_t bitpos = h % bits;
        array[bitpos/8] |= (1 << (bitpos % 8)); // NOTE: htt, 将 hash对应的位设置为true
        h += delta;
      }
    }
  }

  virtual bool KeyMayMatch(const Slice& key, const Slice& bloom_filter) const { // NOTE: htt, 判断key是否满足bloom过滤器
    const size_t len = bloom_filter.size();
    if (len < 2) return false;

    const char* array = bloom_filter.data();
    const size_t bits = (len - 1) * 8; // NOTE: htt, 最后一个字节为 k_

    // Use the encoded k so that we can read filters generated by
    // bloom filters created using different parameters.
    const size_t k = array[len-1]; // NOTE: htt, 获取k_
    if (k > 30) {
      // Reserved for potentially new encodings for short bloom filters.
      // Consider it a match.
      return true; // NOTE: htt, k_不应该大于 30
    }

    uint32_t h = BloomHash(key);
    const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
    for (size_t j = 0; j < k; j++) {
      const uint32_t bitpos = h % bits;
      if ((array[bitpos/8] & (1 << (bitpos % 8))) == 0) return false; // NOTE: htt, bloom只要有一位为false，则返回false
      h += delta;
    }
    return true;
  }
};
}

const FilterPolicy* NewBloomFilterPolicy(int bits_per_key) { // NOTE: htt, 生成 bloom filter的策略
  return new BloomFilterPolicy(bits_per_key);
}

}  // namespace leveldb