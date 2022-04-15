// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

namespace leveldb {

struct Table::Rep {
  ~Rep() {
    delete filter;
    delete [] filter_data; // NOTE:htt, 删除filter使用的二进制数据
    delete index_block;
  }

  Options options;
  Status status;
  RandomAccessFile* file; // NOTE:htt, sstable文件读取
  uint64_t cache_id; // NOTE:htt, 新的cache id
  FilterBlockReader* filter;// NOTE:htt, 从已构建的bloomFilter解析列表,然后对相应的key进行bloomFilter查询
  const char* filter_data; // NOTE:htt, 堆分配的filter读取的buf,需要一个删除机制

  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer // NOTE:htt, meta index handle信息
  Block* index_block; // NOTE:htt, index block加载内存
};

Status Table::Open(const Options& options,
                   RandomAccessFile* file,
                   uint64_t size,
                   Table** table) { // NOTE:htt, 读取sstable,构建Table读取
  *table = NULL;
  if (size < Footer::kEncodedLength) {
    return Status::InvalidArgument("file is too short to be an sstable");
  }

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space); // NOTE:htt, 读取footer内容
  if (!s.ok()) return s;

  Footer footer;
  s = footer.DecodeFrom(&footer_input); // NOTE:htt, 解析footer,包括{meta index hander, index hander}
  if (!s.ok()) return s;

  // Read the index block
  BlockContents contents;
  Block* index_block = NULL;
  if (s.ok()) {
    s = ReadBlock(file, ReadOptions(), footer.index_handle(), &contents); // NOTE:htt, 从BlockHandle索引读取指向<offset_, size_>数据
    if (s.ok()) {
      index_block = new Block(contents); // NOTE:htt, 构建index block的读取
    }
  }

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block; // NOTE:htt, 加载内存的index block
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0); // NOTE:htt, 新的cache id
    rep->filter_data = NULL;
    rep->filter = NULL;
    *table = new Table(rep);
    (*table)->ReadMeta(footer);// NOTE:htt, 根据meta index读取meta block, 并构建bloomFilter的解析
  } else {
    if (index_block) delete index_block;
  }

  return s;
}

void Table::ReadMeta(const Footer& footer) { // NOTE:htt, 根据meta index读取meta block, 并构建bloomFilter的解析
  if (rep_->options.filter_policy == NULL) {
    return;  // Do not need any metadata
  }

  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  ReadOptions opt;
  BlockContents contents;
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {// NOTE:htt, meta index读取指向<offset, size>数据
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  Block* meta = new Block(contents); // NOTE:htt, 构建meta block

  Iterator* iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  iter->Seek(key); // NOTE: htt, 找key对应的<key,value>，先在restart索引二分查找,在restart[i]内顺序查找
  if (iter->Valid() && iter->key() == Slice(key)) {
    ReadFilter(iter->value()); // NOTE:htt, 构建bloomFilter的解析
  }
  delete iter;
  delete meta;
}

void Table::ReadFilter(const Slice& filter_handle_value) { // NOTE:htt, 构建bloomFilter的解析
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  BlockContents block;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) { // NOTE:htt, 读取指向<offset, size> meta数据
    return;
  }
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();     // Will need to delete later // NOTE:htt,堆分配filter二进制内容需删除机制
  }
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data); // NOTE:htt, 构建filter block的读取
}

Table::~Table() {
  delete rep_; // NOTE:htt, 删除数据对象
}

static void DeleteBlock(void* arg, void* ignored) { // NOTE:htt, 按Block指针删除数据
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) { // NOTE:htt, 删除缓存的Block
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) { // NOTE:htt, 释放Block Handle
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle); // NOTE:htt, 释放handle,采用引用计数方式,判断是否需要销毁
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
Iterator* Table::BlockReader(void* arg,
                             const ReadOptions& options,
                             const Slice& index_value) { // NOTE:htt, 读取Block,并且根据需要进行缓存
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache; // NOTE:htt, 获取Cache
  Block* block = NULL;
  Cache::Handle* cache_handle = NULL;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input); // NOTE:htt, 解析 index_value,得到{offset, size}
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    BlockContents contents;
    if (block_cache != NULL) {
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer+8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer)); // NOTE:htt, Block缓存的key: cache_id+handle.offset()
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != NULL) { // NOTE:htt, 存储Block的缓存
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle)); // NOTE:htt, 得到缓存的Block
      } else {
        s = ReadBlock(table->rep_->file, options, handle, &contents); // NOTE:htt, 从文件中读取Block
        if (s.ok()) {
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) { // NOTE:htt, 需要缓存时才进行Block缓存
            cache_handle = block_cache->Insert(
                key, block, block->size(), &DeleteCachedBlock); // NOTE:htt, 插入<key, Block>到缓存中,deleter为删除value
          }
        }
      }
    } else {
      s = ReadBlock(table->rep_->file, options, handle, &contents); // NOTE:htt, 直接从文件读取Block
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  Iterator* iter;
  if (block != NULL) {
    iter = block->NewIterator(table->rep_->options.comparator); // NOTE:htt, 构建Block的迭代器
    if (cache_handle == NULL) {
      iter->RegisterCleanup(&DeleteBlock, block, NULL); // NOTE:htt, 注册删除block
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle); // NOTE:htt, 注册缓存清理
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

Iterator* Table::NewIterator(const ReadOptions& options) const { // NOTE:htt, 构建两层迭代器,先找index迭代器,在构建data迭代器
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator), // NOTE:htt, index block迭代器
      &Table::BlockReader, const_cast<Table*>(this), options);
}

Status Table::InternalGet(const ReadOptions& options, const Slice& k,
                          void* arg,
                          void (*saver)(void*, const Slice&, const Slice&)) {// NOTE:htt,先读取index iter,再获取data block,并查找data
  Status s;
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator); // NOTE:htt, index block迭代器
  iiter->Seek(k);
  if (iiter->Valid()) {
    Slice handle_value = iiter->value(); // NOTE:htt, block的索引{offset, size}
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    if (filter != NULL &&
        handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) { // NOTE:htt, bloomFilter过滤不存在则一定不存在
      // Not found
    } else {
      Iterator* block_iter = BlockReader(this, options, iiter->value()); // NOTE:htt, 读取Block,根据需要缓存,构建date block iter
      block_iter->Seek(k); // NOTE:htt, data block中查询
      if (block_iter->Valid()) {
        (*saver)(arg, block_iter->key(), block_iter->value()); // NOTE:htt, 保存查找到<key,value>
      }
      s = block_iter->status();
      delete block_iter; // NOTE:htt, 删除date block iter
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}


uint64_t Table::ApproximateOffsetOf(const Slice& key) const { // NOTE:htt, 找到key所在block偏移,若key不存在,返回meta block index偏移
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator); // NOTE:htt, index block迭代器
  index_iter->Seek(key); // NOTE:htt, 查找到data block的 {offset,size}
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset(); // NOTE:htt, data block在sst文件中偏移
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset(); // NOTE:htt, key未找到,返回近似值,即 meta block index 的偏移
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb
