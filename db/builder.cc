// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/filename.h"
#include "db/dbformat.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

namespace leveldb {

Status BuildTable(const std::string& dbname,
                  Env* env,
                  const Options& options,
                  TableCache* table_cache,
                  Iterator* iter,
                  FileMetaData* meta) { // NOTE:htt, 将iter中的数据内容写入到sst文件中(dbname为前缀),生成元信息到meta
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();

  std::string fname = TableFileName(dbname, meta->number); // NOTE:htt, 表文件名, ${name}/${number}.ldb
  if (iter->Valid()) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    TableBuilder* builder = new TableBuilder(options, file);// NOTE:htt, 完成整个sstable写入,包括{data block列表, meta block, meta index block, index block, footer} 写入
    meta->smallest.DecodeFrom(iter->key()); // NOTE:htt, 设置最小的key
    for (; iter->Valid(); iter->Next()) {
      Slice key = iter->key();
      meta->largest.DecodeFrom(key); // NOTE:htt, 设置最大的key
      builder->Add(key, iter->value()); // NOTE:htt, 将迭代器中的内容写入到 sst 文件中
    }

    // Finish and check for builder errors
    if (s.ok()) {
      s = builder->Finish();// NOTE:htt, 完成整个sstable写入, 包括{data block列表, meta block, meta index block, index block, footer} 写入
      if (s.ok()) {
        meta->file_size = builder->FileSize(); // NOTE:htt, 设置文件大小
        assert(meta->file_size > 0);
      }
    } else {
      builder->Abandon();// NOTE:htt, 废弃当前sstab的写入
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync(); // NOTE: htt, 将目录entry信息刷盘，同时将用户态数据刷入内核，内核态数据刷入磁盘
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = NULL;

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(),
                                              meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->DeleteFile(fname); // NOTE:htt, 异常时删除文件
  }
  return s;
}

}  // namespace leveldb
