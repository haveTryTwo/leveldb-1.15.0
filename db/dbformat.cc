// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/dbformat.h"
#include <stdio.h>
#include "port/port.h"
#include "util/coding.h"

namespace leveldb {

static uint64_t PackSequenceAndType(uint64_t seq, ValueType t) {  // NOTE:htt, 将{seq, t}组合一个值
  assert(seq <= kMaxSequenceNumber);
  assert(t <= kValueTypeForSeek);
  return (seq << 8) | t;  // NOTE:htt, seq占高56位, t占低8位
}

void AppendInternalKey(std::string* result, const ParsedInternalKey& key) {  // NOTE:htt,采用{key, seq, t}构建内部key
  result->append(key.user_key.data(), key.user_key.size());                  // NOTE:htt, 添加 key
  PutFixed64(result, PackSequenceAndType(key.sequence, key.type));           // NOTE:htt, 添加{seq,type}
}

std::string ParsedInternalKey::DebugString() const {  // NOTE:htt, ParsedInternalKey 的debug信息
  char buf[50];
  snprintf(buf, sizeof(buf), "' @ %llu : %d", (unsigned long long)sequence, int(type));
  std::string result = "'";
  result += EscapeString(user_key.ToString());  // NOTE:htt, 转义key为可打印
  result += buf;
  return result;
}

std::string InternalKey::DebugString() const {  // NOTE:htt, 内部key的debug信息
  std::string result;
  ParsedInternalKey parsed;
  if (ParseInternalKey(rep_, &parsed)) {  // NOTE:htt, 从内部key解析出{user_key, seq, t}
    result = parsed.DebugString();        // NOTE:htt, ParseInternalKey的debug信息
  } else {
    result = "(bad)";
    result.append(EscapeString(rep_));
  }
  return result;
}

const char* InternalKeyComparator::Name() const {  // NOTE:htt, 内部key的名称
  return "leveldb.InternalKeyComparator";
}

int InternalKeyComparator::Compare(const Slice& akey,
                                   const Slice& bkey) const {  // NOTE:htt,比较内部key,其中用户key升序,{seq,t}降序
  // Order by:
  //    increasing user key (according to user-supplied comparator)
  //    decreasing sequence number
  //    decreasing type (though sequence# should be enough to disambiguate)
  int r =
      user_comparator_->Compare(ExtractUserKey(akey), ExtractUserKey(bkey));  // NOTE:htt, 先比较用户key, 用户key升序
  if (r == 0) {
    const uint64_t anum = DecodeFixed64(akey.data() + akey.size() - 8);  // NOTE:htt, {seq, t}组合
    const uint64_t bnum = DecodeFixed64(bkey.data() + bkey.size() - 8);  // NOTE:htt, {seq, t}组合
    if (anum > bnum) {                                                   // NOTE:htt, {seq,t}降序
      r = -1;
    } else if (anum < bnum) {
      r = +1;
    }
  }
  return r;
}

void InternalKeyComparator::FindShortestSeparator(std::string* start,
                                                  const Slice& limit)
    const {  // NOTE:htt,找到比内部key
             // start大,比limit小的最小字符串,如{abcg,seq,t}和{abmn,seq,t},则字符串{abd,MaxSeq,ValueType}
  // Attempt to shorten the user portion of the key
  Slice user_start = ExtractUserKey(*start);  // NOTE:htt, 从内部key中取出用户key
  Slice user_limit = ExtractUserKey(limit);   // NOTE:htt, 从内部key中取出用户key
  std::string tmp(user_start.data(), user_start.size());
  user_comparator_->FindShortestSeparator(&tmp, user_limit);
  if (tmp.size() < user_start.size() && user_comparator_->Compare(user_start, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp, PackSequenceAndType(kMaxSequenceNumber,
                                         kValueTypeForSeek));  // NOTE:htt, {tmp,MaxSeq,ValueType}组合新内部key
    assert(this->Compare(*start, tmp) < 0);
    assert(this->Compare(tmp, limit) < 0);
    start->swap(tmp);
  }
}

void InternalKeyComparator::FindShortSuccessor(
    std::string* key) const {  // NOTE: htt,找到比内部key大最小字符串，比如{amn,seq,t},对应字符串为{b,seq,t}
  Slice user_key = ExtractUserKey(*key);  // NOTE:htt, 从内部key中取出用户key
  std::string tmp(user_key.data(), user_key.size());
  user_comparator_->FindShortSuccessor(&tmp);
  if (tmp.size() < user_key.size() && user_comparator_->Compare(user_key, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp, PackSequenceAndType(kMaxSequenceNumber,
                                         kValueTypeForSeek));  // NOTE:htt, {tmp,MaxSeq,ValueType}组合新内部key
    assert(this->Compare(*key, tmp) < 0);
    key->swap(tmp);
  }
}

const char* InternalFilterPolicy::Name() const {  // NOTE:htt, 内部过滤器名称
  return user_policy_->Name();
}

void InternalFilterPolicy::CreateFilter(const Slice* keys, int n,
                                        std::string* dst) const {  // NOTE:htt, 从内部keys数组构建过滤器
  // We rely on the fact that the code in table.cc does not mind us
  // adjusting keys[].
  Slice* mkey = const_cast<Slice*>(keys);
  for (int i = 0; i < n; i++) {
    mkey[i] = ExtractUserKey(keys[i]);  // NOTE:htt, 从内部keys数组获取 用户key数组
    // TODO(sanjay): Suppress dups?
  }
  user_policy_->CreateFilter(keys, n, dst);  // NOTE:htt, 将用户keys数组生成过滤器
}

bool InternalFilterPolicy::KeyMayMatch(const Slice& key,
                                       const Slice& f) const {  // NOTE:htt, 采用内部key时转换为用户key进行比较
  return user_policy_->KeyMayMatch(ExtractUserKey(key), f);  // NOTE:htt, 抽取用户key进行比较
}

LookupKey::LookupKey(const Slice& user_key,
                     SequenceNumber s) {  // NOTE:htt, 通过{user_key, seq, ValueType}构建LookupKey
  size_t usize = user_key.size();
  size_t needed = usize + 13;  // A conservative estimate
  char* dst;
  if (needed <= sizeof(space_)) {  // NOTE:htt, 小数组使用使用栈空间
    dst = space_;
  } else {
    dst = new char[needed];
  }
  start_ = dst;                          // NOTE:htt,字符串起始
  dst = EncodeVarint32(dst, usize + 8);  // NOTE:htt, 存储{user_key,seq,t}长度
  kstart_ = dst;                         // NOTE:htt, userkey起始
  memcpy(dst, user_key.data(), usize);   // NOTE:htt, 存储user_key
  dst += usize;
  EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek));  // NOTE:htt, 存储{seq,t}
  dst += 8;
  end_ = dst;
}

}  // namespace leveldb
