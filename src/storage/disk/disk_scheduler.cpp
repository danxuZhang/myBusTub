//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  //  throw NotImplementedException(
  //      "DiskScheduler is not implemented yet. If you have finished implementing the disk scheduler, please remove the
  //      " "throw exception line in `disk_scheduler.cpp`.");

  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

void DiskScheduler::Schedule(DiskRequest r) { request_queue_.Put(std::make_optional<DiskRequest>(std::move(r))); }

void DiskScheduler::StartWorkerThread() {
  auto request = request_queue_.Get();
  while (true) {
    if (!request.has_value()) {
      break;
    }
    auto is_write = request->is_write_;
    auto page_id = request->page_id_;
    auto data = request->data_;
    auto callback = std::move(request->callback_);

    if (is_write) {
      disk_manager_->WritePage(page_id, data);
    } else {
      disk_manager_->ReadPage(page_id, data);
    }

    callback.set_value(true);
    request = request_queue_.Get();
  }
}

}  // namespace bustub
