/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) { max_size_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;  

/**
 * @description: 使用LRU策略删除一个victim frame，并返回该frame的id
 * @param {frame_id_t*} frame_id 被移除的frame的id，如果没有frame被移除返回nullptr
 * @return {bool} 如果成功淘汰了一个页面则返回true，否则返回false
 */
bool LRUReplacer::victim(frame_id_t *frame_id) {
    std::lock_guard<std::mutex> lock(latch_);
    
    // 如果没有可替换的帧，返回false
    if (lru_list_.empty()) {
        return false;
    }
    
    // 获取最久未使用的帧
    *frame_id = lru_list_.back();
    lru_list_.pop_back();
    lru_map_.erase(*frame_id);
    
    return true;
}

/**
 * @description: 固定指定的frame，即该页面无法被淘汰
 * @param {frame_id_t} 需要固定的frame的id
 */
void LRUReplacer::pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(latch_);
    
    // 如果帧在LRU列表中，将其移除
    if (lru_map_.count(frame_id) > 0) {
        lru_list_.erase(lru_map_[frame_id]);
        lru_map_.erase(frame_id);
    }
}

/**
 * @description: 取消固定一个frame，代表该页面可以被淘汰
 * @param {frame_id_t} frame_id 取消固定的frame的id
 */
void LRUReplacer::unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(latch_);
    
    // 如果帧已经在LRU列表中，不需要重复添加
    if (lru_map_.count(frame_id) > 0) {
        return;
    }
    
    // 将帧添加到LRU列表的头部
    lru_list_.push_front(frame_id);
    lru_map_[frame_id] = lru_list_.begin();
}

/**
 * @description: 获取当前replacer中可以被淘汰的页面数量
 */
size_t LRUReplacer::Size() { return LRUlist_.size(); }
