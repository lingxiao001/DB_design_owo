/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_scan.h"
#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // 初始化扫描位置
    rid_ = {0, 0};
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    // 获取当前页面
    PageId page_id = {file_handle_->fd_, rid_.page_no};
    Page *page = file_handle_->buffer_pool_manager_->fetch_page(page_id);
    if (page == nullptr) {
        rid_ = {-1, -1};  // 标记扫描结束
        return;
    }
    
    // 在当前页面中查找下一个有效记录
    while (true) {
        // 检查是否到达页面末尾
        if (rid_.slot_no >= file_handle_->file_hdr_.num_records_per_page) {
            // 取消固定当前页面
            file_handle_->buffer_pool_manager_->unpin_page(page_id, false);
            
            // 移动到下一页
            rid_.page_no++;
            rid_.slot_no = 0;
            
            // 检查是否到达文件末尾
            if (rid_.page_no >= file_handle_->file_hdr_.num_pages) {
                rid_ = {-1, -1};  // 标记扫描结束
                return;
            }
            
            // 获取新页面
            page_id = {file_handle_->fd_, rid_.page_no};
            page = file_handle_->buffer_pool_manager_->fetch_page(page_id);
            if (page == nullptr) {
                rid_ = {-1, -1};  // 标记扫描结束
                return;
            }
        }
        
        // 检查当前槽位是否有效
        char *slot = page->data_ + rid_.slot_no * file_handle_->file_hdr_.record_size;
        if (slot[0] != 0) {  // 非0表示有效记录
            // 取消固定页面
            file_handle_->buffer_pool_manager_->unpin_page(page_id, false);
            return;
        }
        
        // 移动到下一个槽位
        rid_.slot_no++;
    }
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    return rid_.page_no == -1 && rid_.slot_no == -1;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    return rid_;
}