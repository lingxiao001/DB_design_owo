/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "buffer_pool_manager.h"

/**
 * @description: 从free_list或replacer中得到可淘汰帧页的 *frame_id
 * @return {bool} true: 可替换帧查找成功 , false: 可替换帧查找失败
 * @param {frame_id_t*} frame_id 帧页id指针,返回成功找到的可替换帧id
 */
bool BufferPoolManager::find_victim_page(frame_id_t* frame_id) {
    // 尝试从空闲列表中获取帧
    if (!free_list_.empty()) {
        *frame_id = free_list_.front();
        free_list_.pop_front();
        return true;
    }
    
    // 使用LRU替换策略选择要淘汰的帧
    return replacer_->victim(frame_id);
}

/**
 * @description: 更新页面数据, 如果为脏页则需写入磁盘，再更新为新页面，更新page元数据(data, is_dirty, page_id)和page table
 * @param {Page*} page 写回页指针
 * @param {PageId} new_page_id 新的page_id
 * @param {frame_id_t} new_frame_id 新的帧frame_id
 */
void BufferPoolManager::update_page(Page *page, PageId new_page_id, frame_id_t new_frame_id) {
    // 如果页面是脏页，将其写回磁盘
    if (page->is_dirty_) {
        disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);
    }
    
    // 更新页表
    page_table_.erase(page->id_);
    page_table_[new_page_id] = new_frame_id;
    
    // 更新页面信息
    page->id_ = new_page_id;
    page->is_dirty_ = false;
}

/**
 * @description: 从buffer pool获取需要的页。
 *              如果页表中存在page_id（说明该page在缓冲池中），并且pin_count++。
 *              如果页表不存在page_id（说明该page在磁盘中），则找缓冲池victim page，将其替换为磁盘中读取的page，pin_count置1。
 * @return {Page*} 若获得了需要的页则将其返回，否则返回nullptr
 * @param {PageId} page_id 需要获取的页的PageId
 */
Page* BufferPoolManager::fetch_page(PageId page_id) {
    // 检查页面是否在缓冲池中
    if (page_table_.count(page_id) > 0) {
        frame_id_t frame_id = page_table_[page_id];
        Page *page = &pages_[frame_id];
        page->pin_count_++;
        replacer_->pin(frame_id);
        return page;
    }
    
    // 获取一个可用的帧
    frame_id_t frame_id;
    if (!find_victim_page(&frame_id)) {
        return nullptr;
    }
    
    // 从磁盘读取页面
    Page *page = &pages_[frame_id];
    disk_manager_->read_page(page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);
    
    // 更新页面信息
    page->id_ = page_id;
    page->pin_count_ = 1;
    page->is_dirty_ = false;
    
    // 更新页表
    page_table_[page_id] = frame_id;
    
    return page;
}

/**
 * @description: 取消固定pin_count>0的在缓冲池中的page
 * @return {bool} 如果目标页的pin_count<=0则返回false，否则返回true
 * @param {PageId} page_id 目标page的page_id
 * @param {bool} is_dirty 若目标page应该被标记为dirty则为true，否则为false
 */
bool BufferPoolManager::unpin_page(PageId page_id, bool is_dirty) {
    // 检查页面是否在缓冲池中
    if (page_table_.count(page_id) == 0) {
        return false;
    }
    
    frame_id_t frame_id = page_table_[page_id];
    Page *page = &pages_[frame_id];
    
    // 更新页面状态
    if (is_dirty) {
        page->is_dirty_ = true;
    }
    
    // 减少pin计数
    if (page->pin_count_ > 0) {
        page->pin_count_--;
    }
    
    // 如果pin计数为0，将页面加入LRU替换器
    if (page->pin_count_ == 0) {
        replacer_->unpin(frame_id);
    }
    
    return true;
}

/**
 * @description: 将目标页写回磁盘，不考虑当前页面是否正在被使用
 * @return {bool} 成功则返回true，否则返回false(只有page_table_中没有目标页时)
 * @param {PageId} page_id 目标页的page_id，不能为INVALID_PAGE_ID
 */
bool BufferPoolManager::flush_page(PageId page_id) {
    // 检查页面是否在缓冲池中
    if (page_table_.count(page_id) == 0) {
        return false;
    }
    
    frame_id_t frame_id = page_table_[page_id];
    Page *page = &pages_[frame_id];
    
    // 将页面写回磁盘
    disk_manager_->write_page(page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);
    page->is_dirty_ = false;
    
    return true;
}

/**
 * @description: 创建一个新的page，即从磁盘中移动一个新建的空page到缓冲池某个位置。
 * @return {Page*} 返回新创建的page，若创建失败则返回nullptr
 * @param {PageId*} page_id 当成功创建一个新的page时存储其page_id
 */
Page* BufferPoolManager::new_page(PageId* page_id) {
    // 获取一个可用的帧
    frame_id_t frame_id;
    if (!find_victim_page(&frame_id)) {
        return nullptr;
    }
    
    // 分配新的页面ID
    page_id->fd = fd_;
    page_id->page_no = next_page_id_++;
    
    // 获取页面并初始化
    Page *page = &pages_[frame_id];
    page->reset_memory();
    page->id_ = *page_id;
    page->pin_count_ = 1;
    page->is_dirty_ = false;
    
    // 更新页表
    page_table_[*page_id] = frame_id;
    
    return page;
}

/**
 * @description: 从buffer_pool删除目标页
 * @return {bool} 如果目标页不存在于buffer_pool或者成功被删除则返回true，若其存在于buffer_pool但无法删除则返回false
 * @param {PageId} page_id 目标页
 */
bool BufferPoolManager::delete_page(PageId page_id) {
    // 检查页面是否在缓冲池中
    if (page_table_.count(page_id) == 0) {
        return false;
    }
    
    frame_id_t frame_id = page_table_[page_id];
    Page *page = &pages_[frame_id];
    
    // 如果页面正在被使用，不能删除
    if (page->pin_count_ > 0) {
        return false;
    }
    
    // 从页表中删除
    page_table_.erase(page_id);
    
    // 将帧加入空闲列表
    free_list_.push_back(frame_id);
    
    // 重置页面
    page->reset_memory();
    
    return true;
}

/**
 * @description: 将buffer_pool中的所有页写回到磁盘
 * @param {int} fd 文件句柄
 */
void BufferPoolManager::flush_all_pages(int fd) {
    // 遍历所有页面
    for (auto &pair : page_table_) {
        PageId page_id = pair.first;
        if (page_id.fd == fd) {
            flush_page(page_id);
        }
    }
}