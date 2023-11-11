//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.h
//
// Identification: src/include/storage/disk/disk_scheduler.h
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <future>  // NOLINT
#include <optional>
#include <thread>  // NOLINT

#include "common/channel.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

/**
 * @brief Represents a Write or Read request for the DiskManager to execute.
 */
struct DiskRequest {
  /** Flag indicating whether the request is a write or a read. */
  bool is_write_;

  /**
   *  Pointer to the start of the memory location where a page is either:
   *   1. being read into from disk (on a read).
   *   2. being written out to disk (on a write).
   */
  char *data_;

  /** ID of the page being read from / written to disk. */
  page_id_t page_id_;

  /** Callback used to signal to the request issuer when the request has been completed. */
  std::promise<bool> callback_;
};

/**
 * @brief The DiskScheduler schedules disk read and write operations.
 *
 * A request is scheduled by calling DiskScheduler::Schedule() with an appropriate DiskRequest object. The scheduler
 * maintains a background worker thread that processes the scheduled requests using the disk manager. The background
 * thread is created in the DiskScheduler constructor and joined in its destructor.
 *
 * This Scheduler is optimized with parallel IO, the default number of worker threads is 4.
 */
class DiskScheduler {
 public:
  explicit DiskScheduler(DiskManager *disk_manager, int num_workers = 4);
  ~DiskScheduler();

  /**
   * @brief Schedules a request for the DiskManager to execute.
   *
   * @param r The request to be scheduled.
   */
  void Schedule(DiskRequest r);

  /**
   * @brief Background worker thread function that processes scheduled requests.
   *
   * The background thread needs to process requests while the DiskScheduler exists, i.e., this function should not
   * return until ~DiskScheduler() is called. At that point you need to make sure that the function does return.
   */
  void StartWorkerThread();

  /**
   * @brief Starts multiple worker threads for processing scheduled requests.
   *
   * This function initiates a specified number of worker threads, each functioning
   * similarly to StartWorkerThread(). These threads collectively handle the
   * processing of scheduled requests. The function ensures that all initiated
   * threads are active and processing tasks as long as the DiskScheduler is in
   * existence.
   *
   * @param num_threads The number of worker threads to start. The default value is 1.
   *                    If a value greater than 1 is provided, that many worker threads
   *                    will be initiated to process requests in parallel, enhancing
   *                    the throughput and efficiency of the request handling process.
   *
   * @note The worker threads created by this function will continue to run and process
   *       requests until the destructor of DiskScheduler (~DiskScheduler()) is called.
   *       Upon calling the destructor, it is essential to ensure that all worker
   *       threads are gracefully terminated, meaning they should complete any ongoing
   *       processing and exit their execution loops.
   *
   */
  void StartMultiWorkerThreads(int num_threads = 1);

  using DiskSchedulerPromise = std::promise<bool>;

  /**
   * @brief Create a Promise object. If you want to implement your own version of promise, you can change this function
   * so that our test cases can use your promise implementation.
   *
   * @return std::promise<bool>
   */
  auto CreatePromise() -> DiskSchedulerPromise { return {}; };

 private:
  /** Pointer to the disk manager. */
  DiskManager *disk_manager_ __attribute__(());
  /** A shared queue to concurrently schedule and process requests. When the DiskScheduler's destructor is called,
   * `std::nullopt` is put into the queue to signal to the background thread to stop execution. */
  Channel<std::optional<DiskRequest>> request_queue_;
  /** A container for holding and managing worker thread objects. */
  std::vector<std::thread> worker_threads_;
};
}  // namespace bustub
