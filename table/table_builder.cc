// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <assert.h>
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {  // NOTE:htt, tableBuilder构建sstable需要的变量信息
  Options options;
  Options index_block_options;  // NOTE:htt, index block块写入options
  WritableFile* file;           // NOTE:htt, 当前sstable写入的文件
  uint64_t offset;              // NOTE:htt, 当前data block在sstable中的偏移
  Status status;
  BlockBuilder data_block;           // NOTE:htt, 数据块构建
  BlockBuilder index_block;          // NOTE:htt, index索引块构建
  std::string last_key;              // NOTE:htt, 当前sstable写入的最后key
  int64_t num_entries;               // NOTE:htt, 当前sstable的<key,value>个数
  bool closed;                       // Either Finish() or Abandon() has been called. // NOTE:htt, 当前是否close
  FilterBlockBuilder* filter_block;  // NOTE:htt, 构建过滤器,如BloomFilter过滤器

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;
  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;  // NOTE:htt, 采用压缩算时,临时保存压缩结果

  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == NULL ? NULL : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    index_block_options.block_restart_interval =
        1;  // NOTE:htt, index block块中restart间隔调整为1,即1个key对应1个restart
  }
};

/**
 * base compress
 */
TableBuilder::BaseCompress::BaseCompress(Rep* r, Slice raw, Slice block_contents)
    : r_(r), raw_(raw), block_contents_(block_contents), type_(kNoCompression) {}

TableBuilder::BaseCompress::~BaseCompress() {}

Status TableBuilder::BaseCompress::Compress() { return Status::NotSupported("not support base compress!"); }

/**
 * no compress
 */
TableBuilder::NoCompress::NoCompress(Rep* r, Slice raw, Slice block_contents) : BaseCompress(r, raw, block_contents) {
  type_ = kNoCompression;
}

TableBuilder::NoCompress::~NoCompress() {}

Status TableBuilder::NoCompress::Compress() { /*{{{*/
  block_contents_ = raw_;
  return Status::OK();
} /*}}}*/

/**
 * snappy compress
 */
TableBuilder::SnappyCompress::SnappyCompress(Rep* r, Slice raw, Slice block_contents)
    : BaseCompress(r, raw, block_contents) {
  type_ = kSnappyCompression;
}

TableBuilder::SnappyCompress::~SnappyCompress() {}

Status TableBuilder::SnappyCompress::Compress() { /*{{{*/
  std::string* compressed = &r_->compressed_output;
  if (port::Snappy_Compress(raw_.data(), raw_.size(), compressed) &&
      compressed->size() < raw_.size() - (raw_.size() / 8u)) {  // NOTE:htt,只有压缩率大于1-87.5%,当前block才采用压缩
    block_contents_ = *compressed;
#ifdef _TEST_
    fprintf(stderr, "snappy compressed size:%zu, raw size:%zu, ratio to:%f%%\n", compressed->size(), raw_.size(),
            ((double)compressed->size() / raw_.size()) * 100);
#endif
  } else {
#ifdef _TEST_
    fprintf(stderr, "not support snappy!\n");
    if (port::Snappy_Compress(raw_.data(), raw_.size(), compressed)) {
      fprintf(stderr, "snappy compressed size:%zu, raw size:%zu, ratio to:%f%%\n", compressed->size(), raw_.size(),
              ((double)compressed->size() / raw_.size()) * 100);
    }
#endif
    // Snappy not supported, or compressed less than 12.5%, so just
    // store uncompressed form
    block_contents_ = raw_;  // NOTE:htt,
                             // 压缩率小于1-87.5%,当前block直接用原始数据,压缩率=(1-压缩后大小/压缩前大小)*100%
    type_ = kNoCompression;
  }

  return Status::OK();
} /*}}}*/

/**
 * zstd compress
 */
TableBuilder::ZstdCompress::ZstdCompress(Rep* r, Slice raw, Slice block_contents)
    : BaseCompress(r, raw, block_contents) {
  type_ = kZstdCompression;
}

TableBuilder::ZstdCompress::~ZstdCompress() {}

Status TableBuilder::ZstdCompress::Compress() { /*{{{*/
  std::string* compressed = &r_->compressed_output;
  if (port::Zstd_Compress(raw_.data(), raw_.size(), compressed) &&
      compressed->size() < raw_.size() - (raw_.size() / 8u)) {  // NOTE:htt,只有压缩率大于1-87.5%,当前block才采用压缩
    block_contents_ = *compressed;
#ifdef _TEST_
    fprintf(stderr, "zstd compressed size:%zu, raw size:%zu, ratio to:%f%%\n", compressed->size(), raw_.size(),
            ((double)compressed->size() / raw_.size()) * 100);
#endif
  } else {
#ifdef _TEST_
    fprintf(stderr, "not support zstd!\n");
    if (port::Zstd_Compress(raw_.data(), raw_.size(), compressed)) {
      fprintf(stderr, "zstd compressed size:%zu, raw size:%zu, ratio to:%f%%\n", compressed->size(), raw_.size(),
              ((double)compressed->size() / raw_.size()) * 100);
    }
#endif
    // Zstd not supported, or compressed less than 12.5%, so just
    // store uncompressed form
    block_contents_ = raw_;  // NOTE:htt,
                             // 压缩率小于1-87.5%,当前block直接用原始数据,压缩率=(1-压缩后大小/压缩前大小)*100%
    type_ = kNoCompression;
  }
  return Status::OK();
} /*}}}*/

/**
 * Compress Factory
 */
TableBuilder::BaseCompress* TableBuilder::CompressFactory::CreateCompress(Rep* r, Slice raw, Slice block_contents,
                                                                          CompressionType type) { /*{{{*/
  switch (type) {
    case kNoCompression:
      return new NoCompress(r, raw, block_contents);
    case kSnappyCompression:
      return new SnappyCompress(r, raw, block_contents);
    case kZstdCompression:
      return new ZstdCompress(r, raw, block_contents);
    default:
      break;
  }
  // 默认采用不压缩机制
  return new NoCompress(r, raw, block_contents);
} /*}}}*/

TableBuilder::TableBuilder(const Options& options, WritableFile* file) : rep_(new Rep(options, file)) {
  if (rep_->filter_block != NULL) {
    rep_->filter_block->StartBlock(0);  // NOTE:htt,尝试启动Filter的第一个block
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);       // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;  // NOTE:htt, 删除Filter
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {  // NOTE:htt, 调整Options
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {  // NOTE:htt, 比较器必须一致
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval =
      1;  // NOTE:htt, index block块中restart间隔调整为1,即1个key对应1个restart
  return Status::OK();
}

void TableBuilder::Add(const Slice& key,
                       const Slice& value) {  // NOTE:htt, 写入<key,value>到data block;若data
                                              // block长度大于指定值,将data block写入到sstabl文件中
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);  // NOTE:htt,保证 sstable中key递增
  }

  if (r->pending_index_entry) {  // NOTE:htt, 将上一个data block的索引添加到 index block
    assert(r->data_block.empty());
    r->options.comparator->FindShortestSeparator(
        &r->last_key,
        key);  // NOTE:htt, 找到比start大,比limit小的最小字符串,如abcg 和 abmn，则找到字符串为 abd
    std::string handle_encoding;
    r->pending_handle.EncodeTo(&handle_encoding);
    r->index_block.Add(r->last_key,
                       Slice(handle_encoding));  // NOTE:htt, index block记录data
                                                 // block最后的<last_key, <offset,size>>
    r->pending_index_entry = false;              // NOTE:htt, 重置pending index entry
  }

  if (r->filter_block != NULL) {
    r->filter_block->AddKey(key);  // NOTE:htt, bloomFilter添加key
  }

  r->last_key.assign(key.data(), key.size());  // NOTE:htt, 调整last_key
  r->num_entries++;                            // NOTE:htt, 增加sstable个entry个数
  r->data_block.Add(key, value);               // NOTE:htt, data block添加key

  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();  // NOTE:htt, data block的评估值
  if (estimated_block_size >= r->options.block_size) {                      // NOTE:htt, 当前data block大于默认值
    Flush();  // NOTE:htt, 写入一个data block到文件,并尝试生成当前block的bloomFilter
  }
}

void TableBuilder::Flush() {  // NOTE:htt, 写入一个data block到文件,并尝试生成当前block的bloomFilter
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  WriteBlock(&r->data_block,
             &r->pending_handle);  // NOTE:htt, 完成data block构建,将{block,type,crc32}写入文件
  if (ok()) {
    r->pending_index_entry = true;  // NOTE:htt, 下一次操作可以进行 index block的写入
    r->status = r->file->Flush();   // NOTE: htt, 将用户空间(FILE流中数据)刷入到内核空间
  }
  if (r->filter_block != NULL) {
    r->filter_block->StartBlock(r->offset);  // NOTE:htt, 生成对应data block的bloomFilter并添加filter中
  }
}

void TableBuilder::WriteBlock(BlockBuilder* block,
                              BlockHandle* handle) {  // NOTE:htt, 完成block构建,将{block,type,crc32}写入文件
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  Slice raw = block->Finish();  // NOTE: htt, 完成block的生成

  Slice block_contents;
  CompressionType type = r->options.compression;  // NOTE:htt, 压缩算法

  // create compress class
  BaseCompress* compress = CompressFactory::CreateCompress(r, raw, block_contents, type);
  if (compress == NULL) {
    r->status = Status::Corruption("create compress failed!");
    return;
  }

  Status s = compress->Compress();
  if (!s.ok()) {
    r->status = s;
    delete compress;
    return;
  }

  WriteRawBlock(compress->block_contents_, compress->type_,
                handle);         // NOTE:htt, 将 {data block, type, crc32} 写入文件
  r->compressed_output.clear();  // NOTE:htt, 清空临时压缩结果
  block->Reset();                // NOTE:htt, 重置data block

  delete compress;
}

void TableBuilder::WriteRawBlock(const Slice& block_contents, CompressionType type,
                                 BlockHandle* handle) {  // NOTE:htt, 将 {block, type, crc32} 写入文件
  Rep* r = rep_;
  handle->set_offset(r->offset);                // NOTE:htt, 记录data block在sstable中的offset
  handle->set_size(block_contents.size());      // NOTE:htt, 记录当前 data block的字符串长度
  r->status = r->file->Append(block_contents);  // NOTE:htt, 将data block写入文件
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;  // NOTE:htt, 压缩类型
    uint32_t crc = crc32c::Value(block_contents.data(),
                                 block_contents.size());  // NOTE:htt, data block的crc32值
    crc = crc32c::Extend(crc, trailer,
                         1);  // Extend crc to cover block type // NOTE:htt, 加上压缩类型的 crc32值
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));                   // NOTE:htt, 保存crc的掩码
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));  // NOTE:htt, 将 {type, crc32}写入文件
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;  // NOTE:htt, 记录下一个data block在sstable中offset
    }
  }
}

Status TableBuilder::status() const {  // NOTE:htt, 写入状态
  return rep_->status;
}

Status TableBuilder::Finish() {  // NOTE:htt, 完成整个sstable写入, 包括{data block列表, meta block,
                                 // meta index block, index block, footer} 写入
  Rep* r = rep_;
  Flush();  // NOTE:htt, 写入一个data block到文件,并尝试生成当前block的bloomFilter
  assert(!r->closed);
  r->closed = true;  // NOTE:htt, 当前table builder关闭

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  if (ok() && r->filter_block != NULL) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);  // NOTE:htt,
                                          // 生成block对应的bloomFilter,并将bloomFilter写入到sstable中
  }

  // Write metaindex block
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != NULL) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);  // NOTE:htt, 记录{filter名称, {offset,size}}到 meta index block
    }

    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block,
               &metaindex_block_handle);  // NOTE:htt, 完成block构建,将{block,type,crc32}写入文件
  }

  // Write index block
  if (ok()) {
    if (r->pending_index_entry) {  // NOTE:htt, 将最后一个data block的索引添加到 index block
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block,
               &index_block_handle);  // NOTE:htt, 完成index block构建,将{block,type,crc32}写入文件
  }

  // Write footer
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);  // NOTE:htt, 记录 meta index block的{offset, size}
    footer.set_index_handle(index_block_handle);          // NOTE:htt, 记录 index block的{offset, size}
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);  // NOTE:htt, 写入footer 到sstable文件中
    if (r->status.ok()) {
      r->offset += footer_encoding.size();  // NOTE:htt, 记录sstable中的offset
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {  // NOTE:htt, 废弃当前sstab的写入
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const {  // NOTE:htt, 当前sstable写入的entry个数
  return rep_->num_entries;
}

uint64_t TableBuilder::FileSize() const {  // NOTE:htt, 当前sstable的offset
  return rep_->offset;
}

}  // namespace leveldb
