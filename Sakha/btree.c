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
    int32_t             minLocal;   /* Minimal area for slot data */
    int32_t             maxLocal;   /* Maximal area for slot data */
};

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

typedef struct BtreePage* sakhadb_btree_page_t;
struct BtreePage
{
    Pgno            no;
    struct BtreePageHeader* header;
};

struct BtreeIndexData
{
    Pgno        ptr;
    Pgno        data;
};

/****************************** B-tree Section ********************************/

struct Btree
{
    sakhadb_btree_env_t     env;        /* Environment */
    sakhadb_btree_page_t    root;       /* Root page */
};

int btreeCreate(sakhadb_btree_env_t env,
                sakhadb_btree_page_t root,
                sakhadb_btree_t* pBtree)
{
    struct Btree* tree = cpl_allocator_allocate(cpl_allocator_get_default(), sizeof(struct Btree));
    if(!tree)
    {
        SLOG_FATAL("btreeCreate: failed to allocate mamory for Btree. ");
        return SAKHADB_NOMEM;
    }
    
    tree->env = env;
    tree->root = root;
    
    *pBtree = tree;
    
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

typedef struct BtreeSlot sakhadb_btree_slot_t;
struct BtreeSlot
{
    uint16_t    off;
    uint16_t    sz;
};

#define btreeNodeOffset(n, off) ((char*)(n) + (off))
#define btreeGetSlots(n) (sakhadb_btree_slot_t*)btreeNodeOffset((n), (n)->slots_off)

static inline int btreeLoadNode(struct BtreeEnv* env, Pgno no, sakhadb_btree_page_t* pPage)
{
    sakhadb_page_t page;
    int rc = sakhadb_pager_request_page(env->pager, no, &page);
    if(rc)
    {
        SLOG_FATAL("btreeLoadNode: failed to request page for Btree node [%d]", rc);
        return rc;
    }
    
    *pPage = (sakhadb_btree_page_t)page;
    
    return rc;
}

static inline void btreeSaveNode(struct BtreeEnv* env, sakhadb_btree_page_t btPage)
{
    sakhadb_pager_save_page(env->pager, (sakhadb_page_t)btPage);
}

static int btreeAllocateNode(
    struct BtreeEnv* env,
    int flags,
    struct BtreePage** ppPage
)
{
    sakhadb_page_t page;
    int rc = sakhadb_pager_request_free_page(env->pager, &page);
    
    if(rc)
    {
        SLOG_FATAL("btreeCreate: failed to request page for Btree root [%d]", rc);
        return rc;
    }
    
    struct BtreePage* btPage = (struct BtreePage*)page;
    
    struct BtreePageHeader* header = btPage->header;
    header->flags = flags;
    header->reserved = 0;
    header->nslots = 0;
    header->free_off = sizeof(struct BtreePageHeader);
    header->slots_off = sakhadb_pager_page_size(env->pager, page->no);
    header->free_sz = header->slots_off - header->free_off;
    header->right = 0;
    
    *ppPage = btPage;
    
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
    sakhadb_btree_page_t btPage = tree->root;
    cpl_array_push_back(&cursor->a, btPage->no);
    sakhadb_btree_node_t node = btPage->header;
    while(1)
    {
        Pgno no = 0;
        int islot = 0;
        int is_leaf = node->flags & SAKHADB_BTREE_LEAF;
        
        if(node->nslots == 0)
        {
            assert(is_leaf);
            cursor->cmp = 1;
            break;
        }
        else
        {
            int cmp = btreeFindKey(node, key, nKey, &islot);
            cursor->islot = islot;
            /* We found a key, that could be more, less or equal. */
            if(cmp == 0)
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
        
        int rc = btreeLoadNode(tree->env, no, &btPage);
        if(rc)
        {
            SLOG_FATAL("btreeSearch: failed to load node. [%d]", rc);
            return rc;
        }
        
        cpl_array_push_back(&cursor->a, no);
        node = btPage->header;
    }
    
    return SAKHADB_NOTFOUND;
}

static inline void btreeInsertNewIndexSlot(sakhadb_btree_node_t node, int islot,
                                           void* key, size_t nkey,
                                           struct BtreeIndexData* data)
{
    SLOG_BTREE_INFO("btreeInsertNewSlot: insert new slot [i:%d][nkey:%lu][data:%d]", islot, nkey, data->data);
    assert(node->free_sz >= nkey + 2*sizeof(Pgno) + sizeof(sakhadb_btree_slot_t));
    
    sakhadb_btree_slot_t* slots = btreeGetSlots(node);
    memmove(slots-1, slots, islot * sizeof(sakhadb_btree_slot_t));
    slots -= 1;
    node->slots_off -= sizeof(sakhadb_btree_slot_t);
    node->nslots += 1;
    
    sakhadb_btree_slot_t* new_slot = slots + islot;
    new_slot->off = node->free_off;
    new_slot->sz = nkey;
    node->free_off += nkey + 2*sizeof(Pgno);
    node->free_sz -= nkey + 2*sizeof(Pgno);
    
    char* offset = btreeNodeOffset(node, new_slot->off);
    memcpy(offset, key, nkey);
    
    offset += nkey;
    memcpy(offset, data, sizeof(struct BtreeIndexData));
}

/**
 * Algorithm:
 * 1. Allocate left and right descendants
 * 2. Choose middle key
 * 3. Copy all lower keys into left descendant
 * 4. Copy all upper keys into right descendant
 * 5. Align keys in root
 * 6. Copy ptr to left descendant from middle key to right-most ptr of left descendant
 * 7. Set ptr of middle key to left descendant
 * 8. Set right-most ptr of root to right descendant
 */
static inline int btreeSplitRoot(struct BtreePage* root, struct BtreeEnv* env)
{
    // p. 1
    sakhadb_btree_page_t left_page = 0;
    int rc = btreeAllocateNode(env, root->header->flags, &left_page);
    if(rc)
    {
        SLOG_BTREE_ERROR("btreeSplitRoot: failed to allocate left node.");
        return rc;
    }
    
    sakhadb_btree_page_t right_page = 0;
    rc = btreeAllocateNode(env, root->header->flags, &right_page);
    if(rc)
    {
        SLOG_BTREE_ERROR("btreeSplitRoot: failed to allocate right node.");
        return rc;
    }
    // p. 1
    
    sakhadb_btree_node_t root_node = root->header;
    sakhadb_btree_slot_t* root_slots = btreeGetSlots(root_node);
    
    sakhadb_btree_node_t left_node = left_page->header;
//    sakhadb_btree_slot_t* left_slots = btreeGetSlots(left_node);
    
    sakhadb_btree_node_t right_node = right_page->header;
//    sakhadb_btree_slot_t* right_slots = btreeGetSlots(right_node);
    
    int is_index = root_node->flags & SAKHADB_BTREE_INDEX;
    
    // p. 2
    size_t middle = (root_node->nslots-1) / 2;
    // p. 2
    
    assert(is_index);
    if(is_index)
    {
        // p. 3
        for (uint16_t i = root_node->nslots - 1; i >= middle + 1; i--) {
            size_t nkey = root_slots[i].sz;
            char* key = btreeNodeOffset(root_node, root_slots[i].off);
            struct BtreeIndexData* data = (struct BtreeIndexData*)btreeGetDataPtr(root_node, i);
            btreeInsertNewIndexSlot(left_node, 0, key, nkey, data);
        }
        left_node->nslots = root_node->nslots - middle - 1;
        // p. 3
        
        // p. 4
        for (uint16_t i = middle-1; i >= 0; i--) {
            size_t nkey = root_slots[i].sz;
            char* key = btreeNodeOffset(root_node, root_slots[i].off);
            struct BtreeIndexData* data = (struct BtreeIndexData*)btreeGetDataPtr(root_node, i);
            btreeInsertNewIndexSlot(right_node, 0, key, nkey, data);
        }
        left_node->nslots = middle;
        // p. 4
    }
    
    
    
    return SAKHADB_OK;
}

static inline int btreeSplitNode(struct BtreePage* node,
                                 struct BtreePage* parent,
                                 struct BtreeEnv* env)
{
    sakhadb_btree_page_t new_node_page;
    int rc = btreeAllocateNode(env, node->header->flags, &new_node_page);
    if(rc)
    {
        return rc;
    }
    
    
    
    sakhadb_pager_save_page(env->pager, (sakhadb_page_t)new_node_page);
    
    return SAKHADB_OK;
}

static inline int btreeInsertNonFullIndex(int cmp, int islot,
                                          sakhadb_btree_node_t node,
                                          void* key, int32_t nkey,
                                          struct BtreeIndexData* data)
{
    switch (cmp) {
        case 0:
        case -1:
            // Standard insert
            btreeInsertNewIndexSlot(node, islot, key, nkey, data);
            break;
            
        case 1:
            // Bigger than all. Insert in the end.
            btreeInsertNewIndexSlot(node, 0, key, nkey, data);
            break;
            
        default:
            break;
    }
    
    return SAKHADB_OK;
}

static inline int btreeInsertIndex(struct BtreeCursor* cursor,
                                   void* key, int32_t nkey,
                                   Pgno data)
{
    SLOG_BTREE_INFO("btreeInsertIndex: insert new key [nkey:%lu][data:%d]", nkey, data);
    struct BtreeEnv* env = cursor->tree->env;
    
    size_t depth = cpl_array_count(&cursor->a);
    int is_root = (depth == 1);
    
    Pgno no = cpl_array_back(&cursor->a, Pgno);
    sakhadb_btree_page_t node_page = 0;
    int rc = btreeLoadNode(cursor->tree->env, no, &node_page);
    if(rc)
    {
        SLOG_BTREE_ERROR("btreeInsertIndex: failed to load node with [Pgno:%d]", no);
        return rc;
    }
    
    sakhadb_btree_node_t node = node_page->header;
    size_t min_entry_size = sizeof(sakhadb_btree_slot_t) + nkey + sizeof(Pgno) + sizeof(int32_t);
    if(node->free_sz < min_entry_size)
    {
        // TODO: split node
        if(is_root)
        {
            rc = btreeSplitRoot(node_page, env);
        }
        else
        {
            Pgno no = cpl_array_get(&cursor->a, depth - 2, Pgno);
            sakhadb_btree_page_t parent_node_page;
            rc = btreeLoadNode(env, no, &parent_node_page);
            if(rc)
            {
                SLOG_BTREE_ERROR("btreeInsertIndex: failed to load node with [Pgno:%d]", no);
                return rc;
            }
            rc = btreeSplitNode(node_page, parent_node_page, env);
        }
        
        if(rc)
        {
            SLOG_BTREE_ERROR("btreeInsertIndex: failed to split node with [Pgno:%d][%d]", no, rc);
            return rc;
        }
        
        
    }
    
    struct BtreeIndexData idata = { 0, no };
    rc = btreeInsertNonFullIndex(cursor->cmp, cursor->islot, node, key, nkey, &idata);
    btreeSaveNode(env, node_page);
    
    return rc;
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
        node->flags = SAKHADB_BTREE_LEAF;
    }
    
    struct Btree* tree;
    rc = btreeCreate(env, metaRoot, &tree);
    if(rc)
    {
        SLOG_FATAL("sakhadb_btree_env_create: failed to create meta B-tree. [%d]", rc);
        goto create_pager_failed;
    }
    
    env->metaBtree = tree;
    
    int32_t usableSize = (int32_t)(sakhadb_pager_page_size(env->pager, 0) - sizeof(struct BtreePageHeader));
    
    env->minLocal = (usableSize * 32/255) - sizeof(sakhadb_btree_slot_t);
    env->maxLocal = (usableSize * 64/255) - sizeof(sakhadb_btree_slot_t);
    
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

int sakhadb_btree_insert(sakhadb_btree_t t, void* key, int32_t nkey, void* data, int32_t ndata)
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
        case SAKHADB_NOTFOUND:
            assert(t->root->header->flags & SAKHADB_BTREE_INDEX);
            assert(ndata == sizeof(Pgno));
            rc = btreeInsertIndex(cursor, key, nkey, *(Pgno*)data);
            break;
            
        default:
            break;
    }
    sakhadb_btree_cursor_destroy(cursor);
    
    return rc;
}

