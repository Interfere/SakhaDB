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

#define SAKHADB_BTREE_LEAF      0x1
#define SAKHADB_BTREE_INDEX     0x2

/***************************** Private Interface ******************************/
struct BtreeEnv
{
    sakhadb_pager_t     pager;      /* Pager is a low-level interface for per-page access */
    struct Btree*       metaBtree;  /* Pointer to the Btree that contains DB meta info */
};

/****************************** B-tree Section ********************************/

struct Btree
{
    sakhadb_btree_env_t     env;        /* Environment */
    Pgno                    rootPageNo; /* Page of root */
    sakhadb_btree_node_t    root;       /* Root node */
};

int btreeCreate(struct BtreeEnv* env,
                struct BtreePageHeader* root,
                Pgno no, struct Btree** ppBtree)
{
    struct Btree* tree = cpl_allocator_allocate(cpl_allocator_get_default(), sizeof(struct Btree));
    if(!tree)
    {
        SLOG_FATAL("btreeCreate: failed to allocate mamory for Btree. ");
        return SAKHADB_NOMEM;
    }
    
    tree->env = env;
    tree->root = root;
    tree->rootPageNo = no;
    
    *ppBtree = tree;
    
    return SAKHADB_OK;
}

void btreeDestroy(struct Btree* tree)
{
    cpl_allocator_free(cpl_allocator_get_default(), tree);
}

/******************************************************************************/

struct BtreePageHeader
{
    uint16_t        free_sz;    /* Size of free space in the page */
    uint16_t        free_off;   /* Offset to free area */
    uint16_t        slots_off;  /* Offset to slots array */
    uint16_t        nslots;     /* No of slots. */
    Pgno            right;      /* Right-most leaf */
};

struct BtreeCursor
{
    struct Btree*   tree;
    cpl_array_t     a;
    int             islot;
    int             cmp;
};

typedef struct BtreeSlot sakhadb_btree_slot_t;
struct BtreeSlot
{
    uint16_t    off;
    uint16_t    sz;
};

#define btreeGetSlots(n) (sakhadb_btree_slot_t*)((char*)(n) + (n)->slots_off)

int btreeLoadNode(struct BtreeEnv* env, Pgno no, sakhadb_btree_node_t* pNode)
{
    sakhadb_page_t page;
    int rc = sakhadb_pager_request_page(env->pager, no, &page);
    if(rc)
    {
        SLOG_FATAL("btreeLoadNode: failed to request page for Btree node [%d]", rc);
        return rc;
    }
    
    *pNode = (sakhadb_btree_node_t)page->data;
    
    return rc;
}

int btreeAllocateNode(struct BtreeEnv* env, int flags, struct BtreePageHeader** ppHeader, sakhadb_page_t* pPage)
{
    sakhadb_page_t page;
    int rc = sakhadb_pager_request_free_page(env->pager, &page);
    
    if(rc)
    {
        SLOG_FATAL("btreeCreate: failed to request page for Btree root [%d]", rc);
        return rc;
    }
    
    *pPage = page;
    
    struct BtreePageHeader* header = (struct BtreePageHeader*)page->data;
    header->nslots = 0;
    header->free_off = sizeof(struct BtreePageHeader);
    header->free_sz = sakhadb_pager_page_size(env->pager, page->no == 1) - header->free_off;
    header->free_sz |= flags;
    header->slots_off = 0;
    header->right = 0;
    
    *ppHeader = header;
    
    return rc;
}

int btreeFindKey(struct BtreePageHeader* node,
                 void* key,
                 uint16_t key_sz,
                 int* pIndex)
{
    if(node->nslots == 0)
    {
        return -1;
    }
    
    sakhadb_btree_slot_t* slots = btreeGetSlots(node);
    sakhadb_btree_slot_t* base = slots;
    register int lim;
    register int cmp;
    register sakhadb_btree_slot_t* slot;
    for(lim = node->nslots; lim != 0; lim>>=1)
    {
        slot = slots + (lim>>1);
        char* stored_key = (char*)node + slot->off;
        cmp = memcmp(key, stored_key, (slot->sz > key_sz)?slot->sz:key_sz);
        if(cmp == 0)
        {
            break;
        }
        if(cmp > 0)
        {
            slots = slot+1;
            lim--;
        }
    }
    
    *pIndex = (int)(slot - base);
    return cmp;
}

void* btreeGetDataPtr(struct BtreePageHeader* node, int islot)
{
    sakhadb_btree_slot_t* slots = btreeGetSlots(node);
    return (char*)node + slots[islot].off + slots[islot].sz;
}

int btreeSearch(struct Btree* tree, void* key, size_t nKey, struct BtreeCursor* cursor)
{
    /* Initialize a cursor. */
    cursor->tree = tree;
    cpl_array_init(&cursor->a, sizeof(Pgno), 64);
    Pgno no = tree->rootPageNo;
    cpl_array_push_back(&cursor->a, no);
    cursor->islot = 0;
 
    sakhadb_btree_node_t node = tree->root;
    while(1)
    {
        int islot = 0;
        int is_leaf = node->free_sz & SAKHADB_BTREE_LEAF;
        int cmp = btreeFindKey(node, key, nKey, &islot);
        cursor->cmp = cmp;
        cursor->islot = islot;
        /* We found a key, that could be more, less or equal. */
        if(cmp == 0 && is_leaf)
        {
            return SAKHADB_OK;
        }
        
        Pgno no;
        
        if(cmp > 0)
        {
            if(is_leaf)
            {
                break;
            }
            
            if(islot == node->nslots - 1)
            {
                no = node->right;
            }
            else
            {
                no = *(Pgno*)btreeGetDataPtr(node, islot+1);
            }
        }
        
        if(cmp < 0)
        {
            if(is_leaf)
            {
                break;
            }
            
            if(islot)
            {
                no = *(Pgno*)btreeGetDataPtr(node, islot);
            }
            else
            {
                no = node->right;
            }
        }
        
        assert(no > 0);
        
        int rc = btreeLoadNode(tree->env, no, &node);
        if(rc)
        {
            SLOG_FATAL("btreeSearch: failed to load node. [%d]", rc);
            return rc;
        }
        
        cpl_array_push_back(&cursor->a, no);
    }
    
    return SAKHADB_OK;
}

/***************************** Public Interface *******************************/
int sakhadb_btree_env_create(sakhadb_file_t __restrict h, sakhadb_btree_env_t* bt)
{
    SLOG_INFO("sakhadb_btree_env_create: creating btree representation");
    cpl_allocator_ref default_allocator = cpl_allocator_get_default();
    struct BtreeEnv* env = (struct BtreeEnv *)cpl_allocator_allocate(default_allocator, sizeof(struct BtreeEnv));
    if(!env)
    {
        SLOG_FATAL("sakhadb_btree_env_create: failed to allocate memory for struct Btree");
        return SAKHADB_NOMEM;
    }
    
    int rc = sakhadb_pager_create(h, &env->pager);
    if(rc)
    {
        SLOG_FATAL("sakhadb_btree_env_create: failed to create pager [code:%d]", rc);
        goto create_pager_failed;
    }
    
    sakhadb_btree_node_t metaRoot;
    rc = btreeLoadNode(env, 1, &metaRoot);
    if(rc)
    {
        SLOG_FATAL("sakhadb_btree_env_create: failed to load Meta Btree root. [%d]", rc);
        goto create_pager_failed;
    }
    
    struct Btree* tree;
    rc = btreeCreate(env, metaRoot, 1, &tree);
    if(rc)
    {
        SLOG_FATAL("sakhadb_btree_env_create: failed to create meta B-tree. [%d]", rc);
        goto create_pager_failed;
    }
    
    env->metaBtree = tree;
    
    *bt = env;
    return SAKHADB_OK;
    
create_pager_failed:
    cpl_allocator_free(default_allocator, env);
    return rc;
}

int sakhadb_btree_env_destroy(sakhadb_btree_env_t bt)
{
    SLOG_INFO("sakhadb_btree_env_destroy: destroying btree representation");
    btreeDestroy(bt->metaBtree);
    int rc = sakhadb_pager_destroy(bt->pager);
    if(rc != SAKHADB_OK)
    {
        SLOG_WARN("sakhadb_btree_env_destroy: failed to destroy pager [%d]", rc);
    }
    
    cpl_allocator_free(cpl_allocator_get_default(), bt);
    return rc;
}

