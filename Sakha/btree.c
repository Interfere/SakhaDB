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
typedef struct BtreeSlot sakhadb_btree_slot_t;
struct BtreeSlot
{
    uint16_t    off;
    uint16_t    sz;
};

#define btreeNodeOffset(n, off) ((char*)(n) + (off))
#define btreeGetSlots(n) (sakhadb_btree_slot_t*)btreeNodeOffset((n), (n)->slots_off)

struct BtreePageHeader
{
    char            flags;              /* Flags */
    char            reserved;
    uint16_t        free_sz;            /* Size of free space in the page */
    uint16_t        free_off;           /* Offset to free area */
    uint16_t        slots_off;          /* Offset to slots array */
    uint16_t        nslots;             /* No of slots. */
    Pgno            right;              /* Right-most leaf */
};

typedef struct BtreePage* sakhadb_btree_page_t;
struct BtreePage
{
    Pgno            no;                 /* Page number */
    struct BtreePageHeader* header;     /* Page Header */
};

struct BtreeContext
{
    sakhadb_pager_t     pager;          /* Pager is a low-level interface for per-page access */
    struct Btree*       metaBtree;      /* Pointer to the Btree that contains DB meta info */
};

struct Btree
{
    sakhadb_btree_ctx_t     ctx;        /* Context */
    sakhadb_btree_page_t    root;       /* Root page */
};

/****************************** B-tree Section ********************************/

static inline int btreeCreate(
    sakhadb_btree_ctx_t ctx,            /* Context */
    sakhadb_btree_page_t root,          /* Root page of the tree */
    sakhadb_btree_t* pBtree
)
{
    struct Btree* tree = cpl_allocator_allocate(cpl_allocator_get_default(), sizeof(struct Btree));
    if(!tree)
    {
        SLOG_FATAL("btreeCreate: failed to allocate mamory for Btree. ");
        return SAKHADB_NOMEM;
    }
    
    tree->ctx = ctx;
    tree->root = root;
    
    *pBtree = tree;
    
    return SAKHADB_OK;
}

static inline void btreeDestroy(struct Btree* tree)
{
    cpl_allocator_free(cpl_allocator_get_default(), tree);
}
/******************************************************************************/


/****************************** Node Section **********************************/

static inline int btreeLoadNode(
    struct BtreeContext * ctx,          /* Context */
    Pgno no,                            /* Page number */
    sakhadb_btree_page_t* pPage
)
{
    sakhadb_page_t page;
    int rc = sakhadb_pager_request_page(ctx->pager, no, &page);
    if(rc)
    {
        SLOG_FATAL("btreeLoadNode: failed to request page for Btree node [%d]", rc);
        return rc;
    }
    
    *pPage = (sakhadb_btree_page_t)page;
    
    return rc;
}

static inline void btreeSaveNode(
    struct BtreeContext * ctx,          /* Context */
    sakhadb_btree_page_t page         /* Page to save */
)
{
    sakhadb_pager_save_page(ctx->pager, (sakhadb_page_t)page);
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
            if(cmp == 0)
            {
                break;
            }
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

static inline void* btreeGetDataPtr(struct BtreePageHeader* node, int islot)
{
    sakhadb_btree_slot_t* slots = btreeGetSlots(node);
    return (char*)node + slots[islot].off + slots[islot].sz;
}

/***************************** Public Interface *******************************/
int sakhadb_btree_ctx_create(sakhadb_file_t __restrict h, sakhadb_btree_ctx_t* ctx)
{
    assert(h);
    assert(ctx);
    
    SLOG_BTREE_INFO("sakhadb_btree_env_create: creating btree representation");
    cpl_allocator_ref default_allocator = cpl_allocator_get_default();
    struct BtreeContext* env = (struct BtreeContext *)cpl_allocator_allocate(default_allocator, sizeof(struct BtreeContext));
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
    
    sakhadb_btree_page_t metaRoot;
    rc = btreeLoadNode(env, 1, &metaRoot);
    if(rc)
    {
        SLOG_FATAL("sakhadb_btree_env_create: failed to load Meta Btree root. [%d]", rc);
        goto create_pager_failed;
    }
    
    sakhadb_btree_node_t node = metaRoot->header;
    
    if(!node->free_sz)
    {
        node->nslots = 0;
        node->slots_off = sakhadb_pager_page_size(env->pager, 1);
        node->free_off = sizeof(struct BtreePageHeader);
        node->free_sz = node->slots_off - sizeof(struct BtreePageHeader);
        node->flags = SAKHADB_BTREE_LEAF | SAKHADB_BTREE_INDEX;
    }
    
    struct Btree* tree;
    rc = btreeCreate(env, metaRoot, &tree);
    if(rc)
    {
        SLOG_FATAL("sakhadb_btree_env_create: failed to create meta B-tree. [%d]", rc);
        goto create_pager_failed;
    }
    
    env->metaBtree = tree;
    
    *ctx = env;
    return SAKHADB_OK;
    
create_pager_failed:
    cpl_allocator_free(default_allocator, env);
    return rc;
}

int sakhadb_btree_ctx_destroy(sakhadb_btree_ctx_t ctx)
{
    assert(ctx);
    
    SLOG_BTREE_INFO("sakhadb_btree_env_destroy: destroying btree representation");
    btreeDestroy(ctx->metaBtree);
    int rc = sakhadb_pager_destroy(ctx->pager);
    if(rc != SAKHADB_OK)
    {
        SLOG_BTREE_WARN("sakhadb_btree_env_destroy: failed to destroy pager [%d]", rc);
    }
    
    cpl_allocator_free(cpl_allocator_get_default(), ctx);
    return rc;
}

sakhadb_btree_t sakhadb_btree_ctx_get_meta(sakhadb_btree_ctx_t ctx)
{
    assert(ctx);
    
    return ctx->metaBtree;
}

int sakhadb_btree_ctx_commit(sakhadb_btree_ctx_t ctx)
{
    assert(ctx);
    
    SLOG_BTREE_INFO("sakhadb_btree_env_commit: commit changes.");
    
    return sakhadb_pager_sync(ctx->pager);
}

