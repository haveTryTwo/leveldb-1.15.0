// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

namespace leveldb {

struct TableAndFile {  // NOTE:htt, sstable 文件和内存 table
  RandomAccessFile* file;
  Table* table;
};

static void DeleteEntry(const Slice& key, void* value) {  // NOTE:htt, 删除 <file, table> entry
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {  // NOTE:htt, 释放handle
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);  // NOTE:htt, 释放 handle
}

TableCache::TableCache(const std::string& dbname, const Options* options, int entries)
    : env_(options->env), dbname_(dbname), options_(options), cache_(NewLRUCache(entries)) {}

TableCache::~TableCache() {
  delete cache_;  // NOTE:htt, 删除cache
}

Status TableCache::FindTable(
    uint64_t file_number, uint64_t file_size,
    Cache::Handle** handle) {  // NOTE:htt, 查找file_number文件,并将{ file_number, {file,table} } 缓存
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if (*handle == NULL) {
    std::string fname = TableFileName(dbname_, file_number);  // NOTE:htt, 表文件名, ${name}/${number}.ldb
    RandomAccessFile* file = NULL;
    Table* table = NULL;
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) {
      std::string old_fname = SSTTableFileName(dbname_, file_number);  // NOTE:htt, sst文件名, ${name}/${number}.sst
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      s = Table::Open(*options_, file, file_size, &table);  // NOTE:htt, 读取sstable,构建Table读取
    }

    if (!s.ok()) {
      assert(table == NULL);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile* tf = new TableAndFile;  // NOTE:htt, 构建 {file, talbe}
      tf->file = file;
      tf->table = table;
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);  // NOTE:htt, 将{ ${number}, {file,table} } 缓存
    }
  }
  return s;
}

Iterator* TableCache::NewIterator(const ReadOptions& options, uint64_t file_number, uint64_t file_size,
                                  Table** tableptr) {  // NOTE:htt, 读取file_number文件,并构建talbe的两层迭代器
  if (tableptr != NULL) {
    *tableptr = NULL;
  }

  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, &handle);  // NOTE:htt, 查找file_number文件,并将其缓存
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;  // NOTE:htt, 获取sstable对应内存table
  Iterator* result = table->NewIterator(options);  // NOTE:htt, 构建两层迭代器,先找index迭代器,在构建data迭代器
  result->RegisterCleanup(&UnrefEntry, cache_, handle);  // NOTE:htt, 注册handle的释放
  if (tableptr != NULL) {
    *tableptr = table;
  }
  return result;
}

Status TableCache::Get(
    const ReadOptions& options, uint64_t file_number, uint64_t file_size, const Slice& k, void* arg,
    void (*saver)(void*, const Slice&,
                  const Slice&)) {  // NOTE:htt, 读取file_number文件,构建table,并查找key,并保存<key,value>
  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, &handle);  // NOTE:htt, 查找file_number文件,并将其缓存
  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;  // NOTE:htt, 获取sstable对应内存table
    s = t->InternalGet(options, k, arg, saver);  // NOTE:htt,先读取index iter,再获取data block,并查找data
    cache_->Release(handle);                     // NOTE:htt, 释放handle
  }
  return s;
}

void TableCache::Evict(uint64_t file_number) {  // NOTE:htt, 释放 file_number对应的{file, table}缓存
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));  // NOTE:htt, 释放 file_number对应的{file, table}缓存
}

}  // namespace leveldb
