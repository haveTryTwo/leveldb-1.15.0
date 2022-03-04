// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/coding.h"

namespace leveldb {

void EncodeFixed32(char* buf, uint32_t value) {
  if (port::kLittleEndian) {
    memcpy(buf, &value, sizeof(value)); // NOTE: htt, 小端模式直接复制
  } else {
    buf[0] = value & 0xff;
    buf[1] = (value >> 8) & 0xff;
    buf[2] = (value >> 16) & 0xff;
    buf[3] = (value >> 24) & 0xff;
  } // NOTE: htt, 大端模式，专门复制
}

void EncodeFixed64(char* buf, uint64_t value) {
  if (port::kLittleEndian) {
    memcpy(buf, &value, sizeof(value)); // NOTE: htt, 小端模式直接复制
  } else {
    buf[0] = value & 0xff;
    buf[1] = (value >> 8) & 0xff;
    buf[2] = (value >> 16) & 0xff;
    buf[3] = (value >> 24) & 0xff;
    buf[4] = (value >> 32) & 0xff;
    buf[5] = (value >> 40) & 0xff;
    buf[6] = (value >> 48) & 0xff;
    buf[7] = (value >> 56) & 0xff;
  } // NOTE: htt, 大端模式，专门复制
}

void PutFixed32(std::string* dst, uint32_t value) {
  char buf[sizeof(value)];
  EncodeFixed32(buf, value);
  dst->append(buf, sizeof(buf)); // NOTE: htt, 将value转变的字符串放入到string中
}

void PutFixed64(std::string* dst, uint64_t value) {
  char buf[sizeof(value)];
  EncodeFixed64(buf, value);
  dst->append(buf, sizeof(buf)); // NOTE: htt, 将value转变的字符串放入到string中
}

char* EncodeVarint32(char* dst, uint32_t v) {
  // Operate on characters as unsigneds
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  static const int B = 128;
  if (v < (1<<7)) { // NOTE: htt, 小于1<<7, 即单个字符即满足
    *(ptr++) = v;
  } else if (v < (1<<14)) {
    *(ptr++) = v | B; // NOTE: htt, 高为补1
    *(ptr++) = v>>7;
  } else if (v < (1<<21)) {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = v>>14;
  } else if (v < (1<<28)) {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = (v>>14) | B;
    *(ptr++) = v>>21;
  } else {
    *(ptr++) = v | B; // NOTE: htt, 高位补1，满足变长变量特性
    *(ptr++) = (v>>7) | B;
    *(ptr++) = (v>>14) | B;
    *(ptr++) = (v>>21) | B;
    *(ptr++) = v>>28;
  }
  return reinterpret_cast<char*>(ptr);
}

void PutVarint32(std::string* dst, uint32_t v) {
  char buf[5]; // NOTE: htt, int类型转变的变长最多为5
  char* ptr = EncodeVarint32(buf, v);
  dst->append(buf, ptr - buf); // NOTE: htt, 将v转变的可变字符串放入到string
}

char* EncodeVarint64(char* dst, uint64_t v) {
  static const int B = 128;
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  while (v >= B) {
    *(ptr++) = (v & (B-1)) | B;
    v >>= 7;
  }
  *(ptr++) = static_cast<unsigned char>(v); // NOTE: htt, 最后一位不补充 
  return reinterpret_cast<char*>(ptr);
}

void PutVarint64(std::string* dst, uint64_t v) {
  char buf[10]; // NOTE: htt, int64转变的可变字符串最多为10位
  char* ptr = EncodeVarint64(buf, v);
  dst->append(buf, ptr - buf); // NOTE: htt, 将v转变的可变字符串放入到string
}

void PutLengthPrefixedSlice(std::string* dst, const Slice& value) { // NOTE: htt，保存长度+字符串类型，其中长度采用可变整数
  PutVarint32(dst, value.size()); // NOTE: htt, 先记录长度，按可变字符存储
  dst->append(value.data(), value.size()); // NOTE: htt, 添加字符串内存
}

int VarintLength(uint64_t v) { // NOTE: htt, 给出v采用可变整数的长度
  int len = 1;
  while (v >= 128) {
    v >>= 7;
    len++;
  }
  return len;
}

const char* GetVarint32PtrFallback(const char* p,
                                   const char* limit,
                                   uint32_t* value) {
  uint32_t result = 0;
  for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
    uint32_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if (byte & 128) { // NOTE: htt, 高位为1，即还未结束
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else { // NOTE: htt, 高位为0，则数字读取完成
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return NULL;
}

bool GetVarint32(Slice* input, uint32_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint32Ptr(p, limit, value);
  if (q == NULL) {
    return false;
  } else {
    *input = Slice(q, limit - q); // NOTE: htt, input内的下一个字符串
    return true;
  }
}

const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value) {
  uint64_t result = 0;
  for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
    uint64_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if (byte & 128) { // NOTE: htt, 高位为1，则未读取完毕
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else { // NOTE: htt, 高位为0，则读取完毕
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return NULL;
}

bool GetVarint64(Slice* input, uint64_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint64Ptr(p, limit, value); // NOTE: htt, 获取可变整数到value
  if (q == NULL) {
    return false;
  } else {
    *input = Slice(q, limit - q); // NOTE: htt, input内部指针指向下一个字符串
    return true;
  }
}

const char* GetLengthPrefixedSlice(const char* p, const char* limit,
                                   Slice* result) { // NOTE: htt, 读取存储的字符串
  uint32_t len;
  p = GetVarint32Ptr(p, limit, &len); // NOTE: htt, 读取字符串的长度
  if (p == NULL) return NULL;
  if (p + len > limit) return NULL;
  *result = Slice(p, len); // NOTE: htt, 根据字符串长度，读取具体的字符串内容
  return p + len;
}

bool GetLengthPrefixedSlice(Slice* input, Slice* result) { // NOTE: htt, 读取存储的字符串
  uint32_t len;
  if (GetVarint32(input, &len) && // NOTE: htt, 读取slice中字符串的长度
      input->size() >= len) {
    *result = Slice(input->data(), len); // NOTE: htt, 根据字符串长度，读取具体的字符串
    input->remove_prefix(len); // NOTE: htt, 调整slice中字符串指针偏移
    return true;
  } else {
    return false;
  }
}

}  // namespace leveldb
