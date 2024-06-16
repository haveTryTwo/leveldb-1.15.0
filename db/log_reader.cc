// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_reader.h"

#include <stdio.h>
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

Reader::Reporter::~Reporter() {}

Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum, uint64_t initial_offset)
    : file_(file),
      reporter_(reporter),
      checksum_(checksum),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      eof_(false),
      last_record_offset_(0),
      end_of_buffer_offset_(0),
      initial_offset_(initial_offset) {}

Reader::~Reader() {
  delete[] backing_store_;  // NOTE:htt, 删除文件
}

bool Reader::SkipToInitialBlock() {  // NOTE:htt, 文件偏移跳转到block整数倍位置
  size_t offset_in_block = initial_offset_ % kBlockSize;
  uint64_t block_start_location = initial_offset_ - offset_in_block;

  // Don't search a block if we'd be in the trailer
  if (offset_in_block > kBlockSize - 6) {  // NOTE:htt, 若块中剩余空间小于7,则跳到下一个Block
    offset_in_block = 0;
    block_start_location += kBlockSize;
  }

  end_of_buffer_offset_ = block_start_location;  // NOTE:htt, end_of_buffer_offset_初始化设置为 init offset对应block偏移

  // Skip to start of first block that can contain the initial record
  if (block_start_location > 0) {
    Status skip_status = file_->Skip(block_start_location);  // NOTE:htt, 文件偏移移动到block起始位置
    if (!skip_status.ok()) {
      ReportDrop(block_start_location, skip_status);  // NOTE:htt, report drop的内容
      return false;
    }
  }

  return true;
}

bool Reader::ReadRecord(Slice* record,
                        std::string* scratch) {  // NOTE:htt, 读取一条完整的日志,Full或<First,Mid,...,Last>组合
  if (last_record_offset_ < initial_offset_) {
    if (!SkipToInitialBlock()) {
      return false;
    }
  }

  scratch->clear();
  record->clear();
  bool in_fragmented_record = false;  // NOTE:htt, 当前日志是否分片,每次读取日志该值为false,只有在读取过程中发生变化
  // Record offset of the logical record that we're reading
  // 0 is a dummy value to make compilers happy
  uint64_t prospective_record_offset = 0;

  Slice fragment;
  while (true) {
    uint64_t physical_record_offset = end_of_buffer_offset_ - buffer_.size();  // NOTE:htt, 记录当前日志在WAL中整体偏移
    const unsigned int record_type = ReadPhysicalRecord(&fragment);  // NOTE:htt, 读取一条满足规范的日志内容
    switch (record_type) {
      case kFullType:
        if (in_fragmented_record) {  // NOTE:htt, 完整日志是没有分片
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (scratch->empty()) {
            in_fragmented_record = false;
          } else {
            ReportCorruption(scratch->size(), "partial record without end(1)");
          }
        }
        prospective_record_offset = physical_record_offset;  // NOTE:htt, 记录最近一条日志WAL偏移
        scratch->clear();
        *record = fragment;                               // NOTE:htt, 设置读取记录内容
        last_record_offset_ = prospective_record_offset;  // NOTE:htt, 设置最近一条日志在WAL中偏移
        return true;

      case kFirstType:
        if (in_fragmented_record) {  // NOTE:htt, 日志开始部分也是没有分片
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (scratch->empty()) {
            in_fragmented_record = false;
          } else {
            ReportCorruption(scratch->size(), "partial record without end(2)");
          }
        }
        prospective_record_offset = physical_record_offset;  // NOTE:htt, 保存当前日志在WAL的起始位置
        scratch->assign(fragment.data(), fragment.size());   // NOTE:htt, scratch保留头部日志部分内容
        in_fragmented_record = true;                         // NOTE:htt, 当前日志分片
        break;

      case kMiddleType:
        if (!in_fragmented_record) {  // NOTE:htt, 读取中间部分时必须是分片
          ReportCorruption(fragment.size(), "missing start of fragmented record(1)");
        } else {
          scratch->append(fragment.data(), fragment.size());  // NOTE:htt, scratch保存中间日志部分内容
        }
        break;

      case kLastType:
        if (!in_fragmented_record) {  // NOTE:htt, 读取到末尾部分说明日志必须分片
          ReportCorruption(fragment.size(), "missing start of fragmented record(2)");
        } else {
          scratch->append(fragment.data(), fragment.size());  // NOTE:htt, scratch保存最后部分日志内容
          *record = Slice(*scratch);                          // NOTE:htt, 将这个日志复制到record中
          last_record_offset_ = prospective_record_offset;    // NOTE:htt, 设置最近一条完整日志在WAL的偏移
          return true;
        }
        break;

      case kEof:
        if (in_fragmented_record) {  // NOTE:htt, 读取完毕时内容应为空,不应分片
          ReportCorruption(scratch->size(), "partial record without end(3)");
          scratch->clear();
        }
        return false;

      case kBadRecord:
        if (in_fragmented_record) {  // NOTE:htt, 异常返回(包括日志在init offset之前),如果有分片则清空分片信息
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        break;

      default: {
        char buf[40];
        snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
        ReportCorruption((fragment.size() + (in_fragmented_record ? scratch->size() : 0)), buf);
        in_fragmented_record = false;  // NOTE:htt, 未知类型清空分片判断
        scratch->clear();
        break;
      }
    }
  }
  return false;
}

uint64_t Reader::LastRecordOffset() {  // NOTE:htt, 最近一条日志在WAL中偏移
  return last_record_offset_;
}

void Reader::ReportCorruption(size_t bytes, const char* reason) {  // NOTE:htt, report corruption信息
  ReportDrop(bytes, Status::Corruption(reason));
}

void Reader::ReportDrop(size_t bytes, const Status& reason) {  // NOTE:htt, report drop的信息
  if (reporter_ != NULL && end_of_buffer_offset_ - buffer_.size() - bytes >=
                               initial_offset_) {  // NOTE:htt, 如果读取内容偏移超过init offset则report
    reporter_->Corruption(bytes, reason);          // NOTE:htt, corrpution异常信息
  }
}

unsigned int Reader::ReadPhysicalRecord(
    Slice* result) {  // NOTE:htt, 读取一条满足记录规范的日志内容,如Full或First或Mid或Last
  while (true) {
    if (buffer_.size() < kHeaderSize) {  // NOTE:htt, buffer内容小于kHeaderSize,才继续读取
      if (!eof_) {
        // Last read was a full read, so this is a trailer to skip
        buffer_.clear();
        Status status =
            file_->Read(kBlockSize, &buffer_, backing_store_);  // NOTE:htt,读WAL整个block,读取前需按init_offset偏移
        end_of_buffer_offset_ += buffer_.size();  // NOTE:htt, 调整end_of_buffer_offset_,加上读取的日志的buffer大小
        if (!status.ok()) {
          buffer_.clear();
          ReportDrop(kBlockSize, status);  // NOTE:htt, report drop的信息
          eof_ = true;
          return kEof;
        } else if (buffer_.size() < kBlockSize) {  // NOTE:htt, 读取内容不足block,则读取完成
          eof_ = true;
        }
        continue;
      } else if (buffer_.size() == 0) {  // NOTE:htt, 如果eof为true,并且buffer长度为0则返回
        // End of file
        return kEof;
      } else {
        size_t drop_size = buffer_.size();
        buffer_.clear();
        ReportCorruption(drop_size, "truncated record at end of file");  // NOTE:htt, report buffer中剩余的内容
        return kEof;
      }
    }

    // Parse the header
    const char* header = buffer_.data();  // NOTE:htt, header指向bufer开始部分
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
    const unsigned int type = header[6];          // NOTE:htt, 当前日志部分类型
    const uint32_t length = a | (b << 8);         // NOTE:htt, 当前日志部分长度
    if (kHeaderSize + length > buffer_.size()) {  // NOTE:htt, kHeaderSize+日志长度不应超过buffer大小
      size_t drop_size = buffer_.size();
      buffer_.clear();
      ReportCorruption(drop_size, "bad record length");  // NOTE:htt, report buffer中异常信息
      return kBadRecord;                                 // NOTE:htt, WAL记录中记录出现异常
    }

    if (type == kZeroType && length == 0) {  // NOTE:htt, 记录长度为0并类型为ZeroType的异常信息
      // Skip zero length record without reporting any drops since
      // such records are produced by the mmap based writing code in
      // env_posix.cc that preallocates file regions.
      buffer_.clear();
      return kBadRecord;
    }

    // Check crc
    if (checksum_) {
      uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
      uint32_t actual_crc = crc32c::Value(header + 6, 1 + length);  // NOTE:htt, 对 ${t}${str} 进行crc
      if (actual_crc != expected_crc) {  // NOTE:htt, 读取内容crc32需和记录crc32保持一致
        // Drop the rest of the buffer since "length" itself may have
        // been corrupted and if we trust it, we could find some
        // fragment of a real log record that just happens to look
        // like a valid log record.
        size_t drop_size = buffer_.size();
        buffer_.clear();
        ReportCorruption(drop_size, "checksum mismatch");
        return kBadRecord;
      }
    }

    buffer_.remove_prefix(kHeaderSize + length);  // NOTE:htt, buffer中跳过当前部分的日志记录

    // Skip physical record that started before initial_offset_
    if (end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length <
        initial_offset_) {  // NOTE:htt, 如果读取内容起始位置比init offset小,则该内容不应该返回
      result->clear();
      return kBadRecord;
    }

    *result = Slice(header + kHeaderSize, length);  // NOTE:htt, 读取日志内容
    return type;                                    // NOTE:htt, 返回当前日志部分的类型
  }
}

}  // namespace log
}  // namespace leveldb
