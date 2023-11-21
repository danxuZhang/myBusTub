//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard_test.cpp
//
// Identification: test/storage/page_guard_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <random>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/page/page_guard.h"

#include "fmt/chrono.h"
#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(PageGuardTest, PageGuardTest) {
  constexpr size_t buffer_pool_size = 5;
  constexpr size_t k = 2;

  const auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  const auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  auto guarded_page = BasicPageGuard(bpm.get(), page0);

  EXPECT_EQ(page0->GetData(), guarded_page.GetData());
  EXPECT_EQ(page0->GetPageId(), guarded_page.PageId());
  EXPECT_EQ(1, page0->GetPinCount());

  guarded_page.Drop();

  EXPECT_EQ(0, page0->GetPinCount());

  auto *page2 = bpm->NewPage(&page_id_temp);
  {
    page2->RLatch();
    auto guard2 = ReadPageGuard(bpm.get(), page2);
    auto guard3 = ReadPageGuard(bpm.get(), page2);
    EXPECT_EQ(1, page2->GetPinCount());
  }
  EXPECT_EQ(0, page2->GetPinCount());

  auto *page = bpm->NewPage(&page_id_temp);
  {
    EXPECT_EQ(1, page->GetPinCount());
    auto guard1 = BasicPageGuard(bpm.get(), page);
    EXPECT_EQ(1, page->GetPinCount());
    auto guard2 = std::move(guard1);
    EXPECT_EQ(1, page->GetPinCount());
  }
  EXPECT_EQ(0, page->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

// NOLINTNEXTLINE
TEST(PageGuardTest, MultiReaderTest) {
  constexpr size_t buffer_pool_size = 10;
  constexpr size_t k = 3;

  const auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  const auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page = bpm->NewPage(&page_id_temp);
  const std::string expected("test");
  snprintf(page->GetData(), BUSTUB_PAGE_SIZE, "%s", expected.c_str());

  std::vector<std::thread> threads;
  constexpr size_t num_threads = 100;

  EXPECT_EQ(1, page->GetPinCount());
  for (size_t i = 0; i < num_threads; ++i) {
    threads.emplace_back([&] {
      auto *page_ptr = bpm->FetchPage(page_id_temp);
      page_ptr->RLatch();
      ReadPageGuard guard(bpm.get(), page_ptr);
      // Perform read operations to test shared access
      const char *data = guard.GetData();
      EXPECT_EQ(expected, std::string(data));
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  EXPECT_EQ(1, page->GetPinCount());
  {
    page->RLatch();
    ReadPageGuard guard(bpm.get(), page);
    EXPECT_EQ(guard.GetData(), expected);
  }
  EXPECT_EQ(0, page->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

// NOLINTNEXTLINE
TEST(PageGuardTest, MultiWriterTest) {
  constexpr size_t buffer_pool_size = 10;
  constexpr size_t k = 3;

  const auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  const auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page = bpm->NewPage(&page_id_temp);

  // append a '#' mark to the end of the text in each thread
  // the number of '#' should be the same as num_threadds

  std::string text;
  snprintf(page->GetData(), BUSTUB_PAGE_SIZE, "");

  std::vector<std::thread> threads;
  constexpr size_t num_threads = 100;

  EXPECT_EQ(1, page->GetPinCount());
  for (size_t i = 0; i < num_threads; ++i) {
    threads.emplace_back([&] {
      auto *page_ptr = bpm->FetchPage(page_id_temp);
      page_ptr->WLatch();
      WritePageGuard guard(bpm.get(), page_ptr);
      // Perform write operations to test shared access
      char *data = guard.GetDataMut();
      std::string s(data);
      s += "#";
      snprintf(data, BUSTUB_PAGE_SIZE, "%s", s.c_str());
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  EXPECT_EQ(1, page->GetPinCount());
  {
    page->RLatch();
    ReadPageGuard guard{bpm.get(), page};
    const auto data = guard.GetData();
    ASSERT_EQ(data, std::string(num_threads, '#'));
  }
  EXPECT_EQ(0, page->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

}  // namespace bustub
