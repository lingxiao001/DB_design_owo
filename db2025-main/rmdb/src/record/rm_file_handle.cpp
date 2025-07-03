/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_file_handle.h"

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid& rid, Context* context) const {
    // 检查rid是否有效
    if (rid.page_no >= file_hdr_.num_pages || rid.slot_no >= file_hdr_.num_records_per_page) {
        return nullptr;
    }
    
    // 获取页面
    PageId page_id = {fd_, rid.page_no};
    Page *page = buffer_pool_manager_->fetch_page(page_id);
    if (page == nullptr) {
        return nullptr;
    }
    
    // 获取记录
    char *slot = page->data_ + rid.slot_no * file_hdr_.record_size;
    std::unique_ptr<RmRecord> record = std::make_unique<RmRecord>(file_hdr_.record_size);
    memcpy(record->data, slot, file_hdr_.record_size);
    
    // 取消固定页面
    buffer_pool_manager_->unpin_page(page_id, false);
    
    return record;
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(char* buf, Context* context) {
    // 查找空闲槽位
    for (page_id_t page_no = 0; page_no < file_hdr_.num_pages; page_no++) {
        PageId page_id = {fd_, page_no};
        Page *page = buffer_pool_manager_->fetch_page(page_id);
        if (page == nullptr) {
            continue;
        }
        
        // 在页面中查找空闲槽位
        for (int slot_no = 0; slot_no < file_hdr_.num_records_per_page; slot_no++) {
            char *slot = page->data_ + slot_no * file_hdr_.record_size;
            if (slot[0] == 0) {  // 0表示空闲槽位
                // 插入记录
                memcpy(slot, buf, file_hdr_.record_size);
                page->is_dirty_ = true;
                
                // 更新文件头
                file_hdr_.first_free_page_no = page_no;
                file_hdr_.num_records++;
                
                // 取消固定页面
                buffer_pool_manager_->unpin_page(page_id, true);
                
                return {page_no, slot_no};
            }
        }
        
        // 取消固定页面
        buffer_pool_manager_->unpin_page(page_id, false);
    }
    
    // 如果没有找到空闲槽位，创建新页面
    PageId page_id = {fd_, file_hdr_.num_pages};
    Page *page = buffer_pool_manager_->new_page(&page_id);
    if (page == nullptr) {
        return {-1, -1};
    }
    
    // 初始化新页面
    memset(page->data_, 0, PAGE_SIZE);
    
    // 插入记录
    memcpy(page->data_, buf, file_hdr_.record_size);
    page->is_dirty_ = true;
    
    // 更新文件头
    file_hdr_.num_pages++;
    file_hdr_.first_free_page_no = file_hdr_.num_pages - 1;
    file_hdr_.num_records++;
    
    // 取消固定页面
    buffer_pool_manager_->unpin_page(page_id, true);
    
    return {file_hdr_.num_pages - 1, 0};
}

/**
 * @description: 在当前表中的指定位置插入一条记录
 * @param {Rid&} rid 要插入记录的位置
 * @param {char*} buf 要插入记录的数据
 */
void RmFileHandle::insert_record(const Rid& rid, char* buf) {
    // 检查rid是否有效
    if (rid.page_no >= file_hdr_.num_pages || rid.slot_no >= file_hdr_.num_records_per_page) {
        return;
    }
    
    // 获取页面
    PageId page_id = {fd_, rid.page_no};
    Page *page = buffer_pool_manager_->fetch_page(page_id);
    if (page == nullptr) {
        return;
    }
    
    // 插入记录
    char *slot = page->data_ + rid.slot_no * file_hdr_.record_size;
    memcpy(slot, buf, file_hdr_.record_size);
    page->is_dirty_ = true;
    
    // 取消固定页面
    buffer_pool_manager_->unpin_page(page_id, true);
}

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
void RmFileHandle::delete_record(const Rid& rid, Context* context) {
    // 检查rid是否有效
    if (rid.page_no >= file_hdr_.num_pages || rid.slot_no >= file_hdr_.num_records_per_page) {
        return;
    }
    
    // 获取页面
    PageId page_id = {fd_, rid.page_no};
    Page *page = buffer_pool_manager_->fetch_page(page_id);
    if (page == nullptr) {
        return;
    }
    
    // 删除记录
    char *slot = page->data_ + rid.slot_no * file_hdr_.record_size;
    slot[0] = 0;  // 标记为空闲槽位
    page->is_dirty_ = true;
    
    // 更新文件头
    file_hdr_.num_records--;
    
    // 取消固定页面
    buffer_pool_manager_->unpin_page(page_id, true);
}


/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {Rid&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
void RmFileHandle::update_record(const Rid& rid, char* buf, Context* context) {
    // 检查rid是否有效
    if (rid.page_no >= file_hdr_.num_pages || rid.slot_no >= file_hdr_.num_records_per_page) {
        return;
    }
    
    // 获取页面
    PageId page_id = {fd_, rid.page_no};
    Page *page = buffer_pool_manager_->fetch_page(page_id);
    if (page == nullptr) {
        return;
    }
    
    // 更新记录
    char *slot = page->data_ + rid.slot_no * file_hdr_.record_size;
    memcpy(slot, buf, file_hdr_.record_size);
    page->is_dirty_ = true;
    
    // 取消固定页面
    buffer_pool_manager_->unpin_page(page_id, true);
}

/**
 * 以下函数为辅助函数，仅提供参考，可以选择完成如下函数，也可以删除如下函数，在单元测试中不涉及如下函数接口的直接调用
*/
/**
 * @description: 获取指定页面的页面句柄
 * @param {int} page_no 页面号
 * @return {RmPageHandle} 指定页面的句柄
 */
RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const {
    // Todo:
    // 使用缓冲池获取指定页面，并生成page_handle返回给上层
    // if page_no is invalid, throw PageNotExistError exception

    return RmPageHandle(&file_hdr_, nullptr);
}

/**
 * @description: 创建一个新的page handle
 * @return {RmPageHandle} 新的PageHandle
 */
RmPageHandle RmFileHandle::create_new_page_handle() {
    // Todo:
    // 1.使用缓冲池来创建一个新page
    // 2.更新page handle中的相关信息
    // 3.更新file_hdr_

    return RmPageHandle(&file_hdr_, nullptr);
}

/**
 * @brief 创建或获取一个空闲的page handle
 *
 * @return RmPageHandle 返回生成的空闲page handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::create_page_handle() {
    // Todo:
    // 1. 判断file_hdr_中是否还有空闲页
    //     1.1 没有空闲页：使用缓冲池来创建一个新page；可直接调用create_new_page_handle()
    //     1.2 有空闲页：直接获取第一个空闲页
    // 2. 生成page handle并返回给上层

    return RmPageHandle(&file_hdr_, nullptr);
}

/**
 * @description: 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 */
void RmFileHandle::release_page_handle(RmPageHandle&page_handle) {
    // Todo:
    // 当page从已满变成未满，考虑如何更新：
    // 1. page_handle.page_hdr->next_free_page_no
    // 2. file_hdr_.first_free_page_no
    
}