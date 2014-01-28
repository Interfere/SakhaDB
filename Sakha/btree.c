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

/**
 * Turn on/off logging for btree routines
 */
#define SLOG_BTREE_ENABLE    1

#if SLOG_BTREE_ENABLE
#   define SLOG_BTREE_INFO  SLOG_INFO
#   define SLOG_BTREE_WARN  SLOG_WARN
#   define SLOG_BTREE_ERROR SLOG_ERROR
#   define SLOG_BTREE_FATAL SLOG_FATAL
#else // SLOG_BTREE_ENABLE
#   define SLOG_BTREE_INFO(...)
#   define SLOG_BTREE_WARN(...)
#   define SLOG_BTREE_ERROR(...)
#   define SLOG_BTREE_FATAL(...)
#endif // SLOG_BTREE_ENABLE

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

/*************************** B-tree Cursor Section ****************************/
struct BtreeCursor
{
    cpl_allocator_ref allocator;
    struct Btree*   tree;
    cpl_array_t     a;
    int             islot;
    int             cmp;
};

int btreeCursorCreate(
    cpl_allocator_ref allocator,            /* Allocator for cursor */
    struct Btree* tree,                     /* Tree, shich cursor would belong to */
    struct BtreeCursor** pCursor            /* Out param */
)
{
    struct BtreeCursor* cursor = cpl_allocator_allocate(allocator, sizeof(struct BtreeCursor));
    if(!cursor)
    {
        SLOG_FATAL("btreeCursorCreate: failed to allocate memory for cursor.");
        return SAKHADB_NOMEM;
    }
    
    cursor->allocator = allocator;
    cursor->tree = tree;
    cpl_array_init(&cursor->a, sizeof(Pgno), 64);
    
    *pCursor = cursor;
    
    return SAKHADB_OK;
}


/******************************************************************************/

struct BtreePageHeader
{
    char            flags;      /* Flags */
    char            reserved;
    uint16_t        free_sz;    /* Size of free space in the page */
    uint16_t        free_off;   /* Offset to free area */
    uint16_t        slots_off;  /* Offset to slots array */
    uint16_t        nslots;     /* No of slots. */
    Pgno            right;      /* Right-most leaf */
};

typedef struct BtreeSlot sakhadb_btree_slot_t;
struct BtreeSlot
{
    uint16_t    off;
    uint16_t    sz;
};

#define btreeNodeOffset(n, off) ((char*)(n) + (off))
#define btreeGetSlots(n) (sakhadb_btree_slot_t*)btreeNodeOffset((n), (n)->slots_off)

static int btreeLoadNode(struct BtreeEnv* env, Pgno no, sakhadb_btree_node_t* pNode)
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

static int btreeAllocateNode(
    struct BtreeEnv* env,
    int flags,
    struct BtreePageHeader** ppHeader,
    sakhadb_page_t* pPage
)
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
    header->flags = flags;
    header->reserved = 0;
    header->nslots = 0;
    header->free_off = sizeof(struct BtreePageHeader);
    header->slots_off = sakhadb_pager_page_size(env->pager, page->no);
    header->free_sz = header->slots_off - header->free_off;
    header->right = 0;
    
    *ppHeader = header;
    
    return rc;
}

static int btreeFindKey(
    struct BtreePageHeader* node,
    void* key,
    uint16_t key_sz,
    int* pIndex
)
{
    sakhadb_btree_slot_t* slots = btreeGetSlots(node);
    sakhadb_btree_slot_t* base = slots;
    register int lim;
    register int cmp;
    register sakhadb_btree_slot_t* slot;
    for(lim = node->nslots; lim != 0; lim>>=1)
    {
        slot = slots + (lim>>1);
        char* stored_key = (char*)node + slot->off;
        cmp = memcmp(key, stored_key, (slot->sz < key_sz)?slot->sz:key_sz);
        if(cmp == 0)
        {
            cmp = key_sz - slots->sz;
        }
        if(cmp == 0)
        {
            break;
        }
        if(cmp < 0)
        {
            slots = slot+1;
            lim--;
        }
    }
    
    if(cmp > 0 && slot != base)
    {
        cmp = -1;
        slot--;
    }
    
    *pIndex = (int)(slot - base);
    return cmp;
}

static void* btreeGetDataPtr(struct BtreePageHeader* node, int islot)
{
    sakhadb_btree_slot_t* slots = btreeGetSlots(node);
    return (char*)node + slots[islot].off + slots[islot].sz;
}

static int btreeSearch(
    struct Btree* tree,
    void* key,
    size_t nKey,
    struct BtreeCursor* cursor
)
{
    Pgno no = tree->rootPageNo;
    cpl_array_push_back(&cursor->a, no);
    sakhadb_btree_node_t node = tree->root;
    while(1)
    {
        Pgno no = 0;
        int islot = 0;
        int is_leaf = node->flags & SAKHADB_BTREE_LEAF;
        
        if(node->nslots == 0)
        {
            cursor->cmp = 1;
            if(is_leaf)
            {
                break;
            }
            
            no = node->right;
        }
        else
        {
            int cmp = btreeFindKey(node, key, nKey, &islot);
            cursor->islot = islot;
            /* We found a key, that could be more, less or equal. */
            if(cmp == 0 && is_leaf)
            {
                cursor->cmp = 0;
                return SAKHADB_OK;
            }
            
            if(cmp > 0)
            {
                cursor->cmp = 1;
                
                if(is_leaf)
                {
                    break;
                }
                
                no = node->right;
            }
            
            if(cmp < 0)
            {
                cursor->cmp = -1;
                
                if(is_leaf)
                {
                    break;
                }
                
                no = *(Pgno*)btreeGetDataPtr(node, islot);
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
    
    return SAKHADB_NOTFOUND;
}

static int btreeSplitNode(struct BtreePageHeader* node, struct BtreeCursor* cursor)
{
    struct Btree* tree = cursor->tree;
    struct BtreeEnv* env = tree->env;
    Pgno no = cpl_array_back(&cursor->a, Pgno);
    sakhadb_btree_node_t parent;
    int rc = btreeLoadNode(env, no, &parent);
    if(rc)
    {
        return rc;
    }
    
    sakhadb_btree_node_t new_node;
    sakhadb_page_t node_page;
    rc = btreeAllocateNode(env, node->flags, &new_node, &node_page);
    if(rc)
    {
        return rc;
    }
    
    new_node->nslots = node->nslots/2;
    sakhadb_btree_slot_t* slots = btreeGetSlots(node);
    sakhadb_btree_slot_t* new_slots = btreeGetSlots(new_node) - new_node->nslots;
    memcpy(slots + node->nslots - new_node->nslots, new_slots, new_node->nslots * sizeof(sakhadb_btree_slot_t));
    memmove(slots + new_node->nslots, slots, (node->nslots - new_node->nslots) * sizeof(sakhadb_btree_slot_t));
    node->nslots -= new_node->nslots;
    
    sakhadb_pager_save_page(env->pager, node_page);
    
    return SAKHADB_OK;
}

static inline void btreeInsertNewSlot(sakhadb_btree_node_t node, int islot,
                                      void* key, size_t nkey,
                                      void* data, size_t ndata)
{
    SLOG_BTREE_INFO("btreeInsertNewSlot: insert new slot [i:%d][nkey:%lu][ndata:%lu]", islot, nkey, ndata);
    assert(node->free_sz >= nkey + ndata + sizeof(sakhadb_btree_slot_t));
    
    sakhadb_btree_slot_t* slots = btreeGetSlots(node);
    memmove(slots-1, slots, islot * sizeof(sakhadb_btree_slot_t));
    slots -= 1;
    node->slots_off -= sizeof(sakhadb_btree_slot_t);
    node->nslots += 1;
    
    sakhadb_btree_slot_t* new_slot = slots + islot;
    new_slot->off = node->free_off;
    new_slot->sz = nkey;
    node->free_off += nkey + ndata;
    node->free_sz -= nkey + ndata;
    
    memcpy(btreeNodeOffset(node, new_slot->off), key, nkey);
    memcpy(btreeNodeOffset(node, new_slot->off + nkey), data, ndata);
}

static int btreeInsertNonFull(struct BtreeCursor* cursor, void* key, size_t nkey,
                              void* data, size_t ndata)
{
    SLOG_BTREE_INFO("btreeInsertNonFull: insert new key [nkey:%lu][ndata:%lu]", nkey, ndata);
    Pgno no = cpl_array_back(&cursor->a, Pgno);
    sakhadb_btree_node_t node = 0;
    int rc = btreeLoadNode(cursor->tree->env, no, &node);
    if(rc)
    {
        SLOG_BTREE_ERROR("btreeInsertNonFull: failed to load node with [Pgno:%d]", no);
        return rc;
    }
    
    switch (cursor->cmp) {
        case -1:
            // Standard insert
            btreeInsertNewSlot(node, cursor->islot, key, nkey, data, ndata);
            break;
            
        case 0:
            // TODO: nothing for a while
            break;
            
        case 1:
            // Bigger than all. Insert in the end.
            btreeInsertNewSlot(node, 0, key, nkey, data, ndata);
            break;
            
        default:
            break;
    }
    
    sakhadb_pager_save_page_no(cursor->tree->env->pager, no);
    
    return SAKHADB_OK;
}

/***************************** Public Interface *******************************/
int sakhadb_btree_env_create(sakhadb_file_t __restrict h, sakhadb_btree_env_t* bt)
{
    assert(h);
    assert(bt);
    
    SLOG_BTREE_INFO("sakhadb_btree_env_create: creating btree representation");
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
    
    if(!metaRoot->free_sz)
    {
        metaRoot->nslots = 0;
        metaRoot->slots_off = sakhadb_pager_page_size(env->pager, 1);
        metaRoot->free_off = sizeof(struct BtreePageHeader);
        metaRoot->free_sz = metaRoot->slots_off - sizeof(struct BtreePageHeader);
        metaRoot->flags = SAKHADB_BTREE_LEAF;
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

int sakhadb_btree_env_destroy(sakhadb_btree_env_t env)
{
    assert(env);
    
    SLOG_BTREE_INFO("sakhadb_btree_env_destroy: destroying btree representation");
    btreeDestroy(env->metaBtree);
    int rc = sakhadb_pager_destroy(env->pager);
    if(rc != SAKHADB_OK)
    {
        SLOG_BTREE_WARN("sakhadb_btree_env_destroy: failed to destroy pager [%d]", rc);
    }
    
    cpl_allocator_free(cpl_allocator_get_default(), env);
    return rc;
}

sakhadb_btree_t sakhadb_btree_env_get_meta(sakhadb_btree_env_t env)
{
    assert(env);
    
    return env->metaBtree;
}

int sakhadb_btree_env_commit(sakhadb_btree_env_t env)
{
    assert(env);
    
    SLOG_BTREE_INFO("sakhadb_btree_env_commit: commit changes.");
    
    return sakhadb_pager_sync(env->pager);
}

sakhadb_btree_cursor_t sakhadb_btree_find(sakhadb_btree_t t, void* key, size_t nkey)
{
    struct BtreeCursor* cursor;
    int rc = btreeCursorCreate(cpl_allocator_get_default(), t, &cursor);
    if(rc)
    {
        return 0;
    }
    
    rc = btreeSearch(t, key, nkey, cursor);
    if(rc)
    {
        sakhadb_btree_cursor_destroy(cursor);
        return 0;
    }
    
    return cursor;
}

void sakhadb_btree_cursor_destroy(sakhadb_btree_cursor_t cursor)
{
    if(cursor)
    {
        cpl_allocator_free(cursor->allocator, cursor);
    }
}

int sakhadb_btree_insert(sakhadb_btree_t t, void* key, size_t nkey, void* data, size_t ndata)
{
    struct BtreeCursor* cursor;
    int rc = btreeCursorCreate(cpl_allocator_get_default(), t, &cursor);
    if(rc)
    {
        return 0;
    }
    
    rc = btreeSearch(t, key, nkey, cursor);
    switch (rc) {
        case SAKHADB_OK:
            // TODO: nothing for a while
            break;
            
        case SAKHADB_NOTFOUND:
            rc = btreeInsertNonFull(cursor, key, nkey, data, ndata);
            break;
            
        default:
            break;
    }
    sakhadb_btree_cursor_destroy(cursor);
    
    return rc;
}

