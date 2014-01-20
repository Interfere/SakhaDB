// Copyright (c) 2013-2014. Alex Komnin. All rights Reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

#include "btree.h"

#include "sakhadb.h"
#include <assert.h>
#include <stdlib.h>
#include <cpl/cpl_allocator.h>
#include <cpl/cpl_array.h>
#include <cpl/cpl_error.h>

#include "logger.h"

enum
{
    KEY_SMALLEST = -1,
    KEY_FOUND    = 0,
    KEY_BIGGER   = 1
};

/***************************** Private Interface ******************************/
struct Btree
{
    sakhadb_pager_t     pager;      /* Pager is a low-level interface for per-page access */
    cpl_array_t         cursors;    /* Array of active cursors */
};

struct BtreePageHeader
{
    uint16_t        free_sz;    /* Size of free space in the page */
    uint16_t        free_off;   /* Offset to free area */
    uint16_t        slots_off;  /* Offset to slots array */
    uint16_t        nslots;     /* No of slots. */
    Pgno            next;       /* Left-most leaf */
};

typedef struct BtreeSlot slot_t;
struct BtreeSlot
{
    uint16_t    off;        /* Offset to the key */
    uint16_t    sz;         /* Size of the key */
};

struct BtreeCursor
{
    struct Btree*   bt;
    Pgno            no;
    slot_t          slot;
};

struct BtreeData {
    Pgno        overflow;   /* Next overflow page */
    int32_t     size;       /* length of data in this page */
    char        data[256];
};

int btreeLoadNode(struct Btree* bt, Pgno no, struct BtreePageHeader** pNode)
{
    sakhadb_page_t page;
    int rc = sakhadb_pager_request_page(bt->pager, no, 0, &page);
    if(rc == SAKHADB_OK)
    {
        *pNode = (struct BtreePageHeader*)page->data;
    }
    return rc;
}

int btreeFindKey(struct BtreePageHeader* node,
                 void* key,
                 uint16_t key_sz,
                 slot_t** pRes)
{
    if(node->nslots == 0)
    {
        *pRes = 0;
        return KEY_SMALLEST;
    }
    
    slot_t* slots = (slot_t*)((char*)node + node->slots_off);
    int lower_bound = 0;
    int upper_bound = node->nslots - 1;
    int middle = upper_bound / 2;
    int last_middle = -1;
    int ret;
    while (upper_bound > lower_bound || last_middle != middle) {
        last_middle = middle;
        slot_t* slot = slots + middle;
        char* stored_key = (char*)node + slot->off;
        int res = memcmp(key, stored_key, (slot->sz > key_sz)?slot->off:key_sz);
        if(res == 0)
        {
            ret = 0;
            break;
        }
        else if(res < 0)
        {
            upper_bound = middle - 1;
            ret = -1;
        }
        else
        {
            lower_bound = middle + 1;
            ret = 1;
        }
        middle = (upper_bound + lower_bound) / 2;
    }
    
    if(ret == -1 && middle > 0)
    {
        middle--;
        ret = KEY_BIGGER;
    }
    
    *pRes = slots + middle;
        
    return ret;
}

static int btreeInsertInNode(struct BtreePageHeader* node, slot_t* slot,
                             void* key, uint16_t nkey,
                             void* data, size_t ndata)
{
    if(node->free_sz > nkey + sizeof(slot_t))
    {
        char* slots = (char*)node + node->slots_off;
        if(slot)
        {
            size_t len = (char*)(slot+1) - slots;
            memmove(slots - sizeof(slot_t), slots, len);
        }
        else
        {
            slot = (slot_t*)(slots - sizeof(slot_t));
        }

        slot->off = node->free_off;
        slot->sz = nkey;
        
        memcpy((char*)node + slot->off, key, nkey);
        
        if(ndata > 256)
        {
            // TODO: make overflow
            assert(0);
        }
        
        memcpy((char*)node + slot->off + nkey, data, ndata);
        
        node->free_off += nkey + ndata;
        node->free_sz -= nkey + ndata + sizeof(slot_t);
        node->slots_off -= sizeof(slot_t);
        node->nslots += 1;
        
        return SAKHADB_OK;
    }
    assert(0);
}

/***************************** Public Interface *******************************/
int sakhadb_btree_create(sakhadb_file_t __restrict h, sakhadb_btree_t* bt)
{
    SLOG_INFO("sakhadb_btree_create: creating btree representation");
    cpl_allocator_ref default_allocator = cpl_allocator_get_default();
    struct Btree* tree = (struct Btree *)cpl_allocator_allocate(default_allocator, sizeof(struct Btree));
    if(!tree)
    {
        SLOG_FATAL("sakhadb_btree_create: failed to allocate memory for struct Btree");
        return SAKHADB_NOMEM;
    }
    int rc = sakhadb_pager_create(h, &tree->pager);
    if(rc)
    {
        SLOG_FATAL("sakhadb_btree_create: failed to create pager [code:%d]", rc);
        goto create_pager_failed;
    }
    
    rc = cpl_array_init(&tree->cursors, _CPL_DEFAULT_ARRAY_SIZE, sizeof(struct BtreeCursor));
    if(rc)
    {
        SLOG_FATAL("sakhadb_btree_create: failed to initialize cursors array [code:%d]", rc);
        goto create_pager_failed;
    }
    
    sakhadb_btree_node_t root;
    rc = sakhadb_btree_get_root(tree, &root);
    if(rc)
    {
        goto create_pager_failed;
    }
    
    if(root->free_sz == 0)
    {
        /* New DB. Create header. */
        size_t pageSize = sakhadb_pager_page_size(tree->pager);
        root->free_off = sizeof(struct BtreePageHeader);
        root->slots_off = pageSize - sakhadb_pager_header_size();
        root->free_sz = root->slots_off - root->free_off;
        root->free_sz |= 1;
    }
    
    *bt = tree;
    return SAKHADB_OK;
    
create_pager_failed:
    cpl_allocator_free(default_allocator, tree);
    return rc;
}

int sakhadb_btree_destroy(sakhadb_btree_t bt)
{
    SLOG_INFO("sakhadb_btree_destroy: destroying btree representation");
    int rc = sakhadb_pager_destroy(bt->pager);
    if(rc != SAKHADB_OK)
    {
        SLOG_WARN("sakhadb_btree_destroy: failed to destroy pager [%d]", rc);
    }
    
    cpl_array_deinit(&(bt->cursors));
    
    cpl_allocator_free(cpl_allocator_get_default(), bt);
    return rc;
}

int sakhadb_btree_get_root(sakhadb_btree_t bt, sakhadb_btree_node_t* root)
{
    return btreeLoadNode(bt, 1, root);
}

sakhadb_btree_cursor_t sakhadb_btree_find_key(
                        sakhadb_btree_t bt,
                        sakhadb_btree_node_t root,
                        void* key, size_t sz
)
{
    sakhadb_btree_cursor_t cursor = 0;
    sakhadb_btree_node_t node = root;
    while (1) {
        int is_leaf = node->free_sz & 1;
        int is_index = node->free_sz & 2;
        slot_t* slot;
        int res = btreeFindKey(node, key, sz, &slot);
        switch (res) {
            case KEY_FOUND:
                if((!is_index) || (is_index && is_leaf))
                {
                    /* create cursor */
                    cpl_array_resize(&bt->cursors, cpl_array_count(&bt->cursors) + 1);
                    cursor = (sakhadb_btree_cursor_t)cpl_array_back_p(&bt->cursors);
                    cursor->bt = bt;
                    cursor->no = 1; // TODO: fetch page number
                    cursor->slot = *slot;
                    goto Lexit;
                }
            case KEY_BIGGER:
                if(!is_leaf)
                {
                    Pgno* pNo = (Pgno *)((char*)node + slot->off + slot->sz);
                    if(btreeLoadNode(bt, *pNo, &node) != SAKHADB_OK)
                    {
                        goto Lexit;
                    }
                    break;
                }
                goto Lexit;
                
            case KEY_SMALLEST:
                if(!is_leaf)
                {
                    if(btreeLoadNode(bt, node->next, &node) != SAKHADB_OK)
                    {
                        goto Lexit;
                    }
                    break;
                }
                goto Lexit;
                
            default:
                assert(0);
        }
    }
    
Lexit:
    return cursor;
}

int sakhadb_btree_insert(sakhadb_btree_t bt, sakhadb_btree_node_t root,
                         void* key, size_t nkey, void* data, size_t ndata)
{
    int rc = SAKHADB_OK;
    sakhadb_btree_node_t node = root;
    while (1) {
        int is_leaf = node->free_sz & 1;
        int is_index = node->free_sz & 2;
        slot_t* slot;
        int res = btreeFindKey(node, key, nkey, &slot);
        switch (res) {
            case KEY_FOUND:
                if(is_index || (!is_index && is_leaf))
                {
                    /* already exists */
                    rc = SAKHADB_FULL;
                    goto Lexit;
                }
            case KEY_BIGGER:
                if(is_leaf)
                {
                    rc = btreeInsertInNode(node, slot, key, nkey, data, ndata);
                }
                else
                {
                    Pgno* pNo = (Pgno *)((char*)node + slot->off + slot->sz);
                    if(btreeLoadNode(bt, *pNo, &node) != SAKHADB_OK)
                    {
                        goto Lexit;
                    }
                    break;
                }
                goto Lexit;
                
            case KEY_SMALLEST:
                if(is_leaf)
                {
                    rc = btreeInsertInNode(node, slot, key, nkey, data, ndata);
                }
                else
                {
                    if(btreeLoadNode(bt, node->next, &node) != SAKHADB_OK)
                    {
                        goto Lexit;
                    }
                    break;
                }
                goto Lexit;
                
            default:
                assert(0);
        }
    }
    
Lexit:
    return rc;
}

int sakhadb_btree_commit(sakhadb_btree_t bt)
{
    int rc = sakhadb_pager_sync(bt->pager);
    
    rc += cpl_array_resize(&bt->cursors, 0);
    
    return rc;
}

