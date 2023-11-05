//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  //  throw NotImplementedException(
  //      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //      "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::ResetPageMetaInFrame(frame_id_t frame_id) -> void {
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> guard{latch_};

  if (free_list_.empty() && replacer_->Size() == 0) {
    return nullptr;
  }

  frame_id_t replacement_frame;
  if (!free_list_.empty()) {
    replacement_frame = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Evict(&replacement_frame);
    if (pages_[replacement_frame].IsDirty()) {
      WritePageToDisk(pages_[replacement_frame].GetPageId());
    }
    page_table_.erase(pages_[replacement_frame].GetPageId());
  }

  *page_id = AllocatePage();

  page_table_[*page_id] = replacement_frame;
  ResetPageMetaInFrame(replacement_frame);
  pages_[replacement_frame].page_id_ = *page_id;
  pages_[replacement_frame].ResetMemory();

  replacer_->RecordAccess(replacement_frame);
  replacer_->SetEvictable(replacement_frame, false);
  pages_[replacement_frame].pin_count_++;

  return &(pages_[replacement_frame]);
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> guard{latch_};
  auto fid_it = page_table_.find(page_id);

  if (fid_it == page_table_.end()) {
    // if the page is not in the buffer pool
    if (free_list_.empty() && replacer_->Size() == 0) {
      return nullptr;
    }

    frame_id_t replacement_frame;
    if (!free_list_.empty()) {
      replacement_frame = free_list_.front();
      free_list_.pop_front();
    } else {
      replacer_->Evict(&replacement_frame);
      page_table_.erase(pages_[replacement_frame].GetPageId());
    }

    if (pages_[replacement_frame].IsDirty()) {
      WritePageToDisk(pages_[replacement_frame].GetPageId());
    }
    ResetPageMetaInFrame(replacement_frame);
    page_table_[page_id] = replacement_frame;
    pages_[replacement_frame].page_id_ = page_id;

    // fetch from the disk
    std::promise<bool> callback;
    std::future<bool> future = callback.get_future();
    DiskRequest read_req{false, pages_[replacement_frame].data_, page_id, std::move(callback)};
    disk_scheduler_->Schedule(std::move(read_req));
    future.get();

    pages_[replacement_frame].pin_count_++;
    replacer_->RecordAccess(replacement_frame);
    replacer_->SetEvictable(replacement_frame, false);

    return &(pages_[replacement_frame]);
  }

  pages_[fid_it->second].pin_count_++;
  replacer_->RecordAccess(fid_it->second);
  replacer_->SetEvictable(fid_it->second, false);
  return &pages_[fid_it->second];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> guard{latch_};
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  frame_id_t fid = page_table_[page_id];
  if (pages_[fid].GetPinCount() == 0) {
    return false;
  }

  pages_[fid].pin_count_--;
  if (!pages_[fid].IsDirty() && is_dirty) {
    pages_[fid].is_dirty_ = true;
  }

  if (pages_[fid].GetPinCount() == 0) {
    replacer_->SetEvictable(fid, true);
  }

  return true;
}

auto BufferPoolManager::WritePageToDisk(page_id_t page_id) -> bool {
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  frame_id_t fid = page_table_.at(page_id);

  std::promise<bool> callback;
  std::future<bool> future = callback.get_future();
  DiskRequest request{true, pages_[fid].data_, page_id, std::move(callback)};
  disk_scheduler_->Schedule(std::move(request));
  future.get();

  pages_[fid].is_dirty_ = false;
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard{latch_};
  return WritePageToDisk(page_id);
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> guard{latch_};
  for (auto &p : page_table_) {
    WritePageToDisk(p.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard{latch_};
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }

  frame_id_t frame_id = page_table_.at(page_id);

  if (pages_[frame_id].GetPinCount() != 0) {
    return false;
  }

  page_table_.erase(page_id);
  free_list_.push_back(frame_id);

  pages_[frame_id].ResetMemory();
  ResetPageMetaInFrame(frame_id);
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
