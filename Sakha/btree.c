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
    Pgno        no;
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
    Pgno                    no;         /* Page number */
    struct BtreePageHeader* header;     /* Page Header */
};

struct BtreeContext
{
    sakhadb_pager_t         pager;      /* Pager is a low-level interface for per-page access */
};

struct Btree
{
    sakhadb_btree_ctx_t     ctx;        /* Context */
    sakhadb_btree_page_t    root;       /* Root page */
};

struct BtreeCursorPointer
{
    sakhadb_btree_page_t    page;
    int                     index;
};

struct BtreeCursorStack
{
    sakhadb_btree_t         tree;
    cpl_array_t             st;         /* stack of cursors */
};

struct BtreeSplitResult
{
    void*                   data;       /* pointer to key data */
    size_t                  size;       /* size of key */
    sakhadb_btree_page_t    new_page;
};

/****************************** B-tree Section ********************************/

static inline int btreeCreate(
    sakhadb_btree_ctx_t ctx,            /* Context */
    sakhadb_btree_page_t root,          /* Root page of the tree */
    sakhadb_btree_t* pBtree
)
{
    SLOG_BTREE_INFO("btreeCreate: create btree [0x%x][0x%x]", ctx, root);
    
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
    SLOG_BTREE_INFO("btreeDestroy: destroy btree [0x%x]", tree->root);
    cpl_allocator_free(cpl_allocator_get_default(), tree);
}

/******************************************************************************/


/****************************** Node Section **********************************/

/*************************** Load/Save Section ********************************/
static inline int btreeLoadNode(
    struct BtreeContext * ctx,          /* Context */
    Pgno no,                            /* Page number */
    sakhadb_btree_page_t* pPage
)
{
    SLOG_BTREE_INFO("btreeLoadNode: load page [0x%x][%d]", ctx, no);
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
    sakhadb_btree_page_t page           /* Page to save */
)
{
    SLOG_BTREE_INFO("btreeSaveNode: save page [%x][%d]", page->no);
    sakhadb_pager_save_page(ctx->pager, (sakhadb_page_t)page);
}

static inline int btreeLoadNewNode(
    struct BtreeContext * ctx,          /* Context */
    char flags,
    sakhadb_btree_page_t* pPage
)
{
    SLOG_BTREE_INFO("btreeLoadNewNode: fetch new node [0x%x][%c]", ctx, flags);
    sakhadb_page_t page;
    int rc = sakhadb_pager_request_free_page(ctx->pager, &page);
    if(rc)
    {
        SLOG_FATAL("btreeLoadNewNode: failed to request page for Btree node [%d]", rc);
        return rc;
    }
    
    register sakhadb_btree_node_t node = page->data;
    node->nslots = 0;
    node->slots_off = sakhadb_pager_page_size(ctx->pager, 0);
    node->free_off = sizeof(struct BtreePageHeader);
    node->free_sz = node->slots_off - sizeof(struct BtreePageHeader);
    node->flags = flags;
    
    *pPage = (sakhadb_btree_page_t)page;
    
    return rc;
}

/****************************** Find Section **********************************/
static inline Pgno btreeGetDataPgno(struct BtreePageHeader* node, int islot)
{
    register sakhadb_btree_slot_t* slots = btreeGetSlots(node);
    return slots[islot].no;
}

static int btreeFindKey(
    sakhadb_btree_node_t node,              /* The node contains slots */
    void* key,                              /* Key to find */
    uint16_t key_sz,                        /* Size of the key to find */
    int* pIndex                             /* Out: index */
)
{
    SLOG_BTREE_INFO("btreeFindKey: find key in node [%hu][%hu]", key_sz, node->nslots);
    sakhadb_btree_slot_t* slots = btreeGetSlots(node);
    sakhadb_btree_slot_t* base = slots;
    register int lim;
    register int cmp = 1;
    register sakhadb_btree_slot_t* slot = slots;
    for(lim = node->nslots; lim != 0; lim>>=1)
    {
        slot = slots + (lim>>1);
        char* stored_key = (char*)node + slot->off;
        cmp = memcmp(key, stored_key, (slot->sz < key_sz)?slot->sz:key_sz);
        if(cmp == 0 && (cmp = key_sz - slots->sz) == 0)
        {
            break;
        }
        if(cmp < 0)
        {
            slots = slot+1;
            lim--;
        }
    }
    
    if(cmp > 0)
    {
        slot--;
    }
 
    *pIndex = (int)(slot - base);
    return cmp;
}

static int btreeFind(
    sakhadb_btree_t tree,                   /* Tree to seek in */
    void* key,                              /* Key to find */
    uint16_t key_sz,                        /* Size of the key to find */
    struct BtreeCursorStack* stack          /* Out param: stack that contains cursors */
)
{
    SLOG_BTREE_INFO("btreeFind: find key in tree [%d][%hu]", tree->root->no, key_sz);
    sakhadb_btree_page_t page = tree->root;
    int cmp;
    while (1) {
        int cur;
        register sakhadb_btree_node_t node = page->header;
        cmp = btreeFindKey(node, key, key_sz, &cur);
        struct BtreeCursorPointer cursor = { page, cur };
        cpl_array_push_back(&stack->st, cursor);
        
        SLOG_BTREE_INFO("btreeFind: finding key in node [%d][%d]", cmp, cur);
        
        if(node->flags)
        {
            break;
        }
        
        Pgno no;
        if(cur == -1)
        {
            no = node->right;
        }
        else
        {
            no  = btreeGetDataPgno(node, cur);
        }
        btreeLoadNode(tree->ctx, no, &page);
    }
    return cmp;
}

/****************************** Split Section *********************************/
static inline void btreeRemoveLastSlot(
    sakhadb_btree_node_t __restrict node
)
{
    assert(node->nslots > 0);
 
    register sakhadb_btree_slot_t* slot = btreeGetSlots(node);
    node->nslots -= 1;
    node->slots_off += sizeof(sakhadb_btree_slot_t);
    node->free_off = slot->off;
    node->free_sz += slot->sz + sizeof(sakhadb_btree_slot_t);
}

static inline void btreeTruncateSlots(
    sakhadb_btree_node_t __restrict node,
    size_t k
)
{
    assert(node->nslots >= k);

    register sakhadb_btree_slot_t* slot = btreeGetSlots(node) + k - 1;
    node->nslots -= k;
    node->slots_off += k * sizeof(sakhadb_btree_slot_t);
    node->free_off = slot->off;
    node->free_sz = node->slots_off - node->free_off;
}

static inline sakhadb_btree_slot_t* btreeAppendSlot(
    sakhadb_btree_node_t __restrict node,
    sakhadb_btree_slot_t* __restrict slot
)
{
    register sakhadb_btree_slot_t* new_slot = btreeGetSlots(node) - 1;
    *new_slot = *slot;
    new_slot->off = node->free_off;
    memmove(btreeNodeOffset(node, new_slot->off),
            btreeNodeOffset(node, slot->off),
            slot->sz);
    
    node->nslots += 1;
    node->free_off += slot->sz;
    node->slots_off -= sizeof(sakhadb_btree_slot_t);
    node->free_sz -= new_slot->sz + sizeof(sakhadb_btree_slot_t);
    
#ifdef DEBUG
    memset(btreeNodeOffset(node, node->free_off), 0xCC, node->free_sz);
#endif
    
    return new_slot;
}

static inline void btreeCopyOnSplit(
    sakhadb_btree_node_t __restrict node,
    sakhadb_btree_node_t __restrict new_node,
    uint16_t k
)
{
    sakhadb_btree_slot_t* slots = btreeGetSlots(node);
    sakhadb_btree_slot_t* new_slots = btreeGetSlots(new_node) - k;
    
    register uint16_t start_off = slots[k-1].off;
    register uint16_t len = slots[0].off + slots[0].sz - start_off;
    memcpy(new_slots, slots, k * sizeof(sakhadb_btree_slot_t));
    memcpy(btreeNodeOffset(new_node, new_node->free_off), btreeNodeOffset(node, start_off), len);
    start_off -= sizeof(struct BtreePageHeader);
    for (uint16_t i = 0; i < k; ++i)
    {
        new_slots[i].off -= start_off;
    }
    new_node->free_off += len;
    new_node->slots_off -= k * sizeof(sakhadb_btree_slot_t);
    new_node->free_sz -= len + k * sizeof(sakhadb_btree_slot_t);
    new_node->nslots = k;
    btreeTruncateSlots(node, k);
}

static inline int btreeSplitNode(
    sakhadb_btree_t tree,
    sakhadb_btree_page_t page,
    struct BtreeSplitResult* res
)
{
    SLOG_BTREE_INFO("btreeSplitNode: split node [%d]", page->no);
    sakhadb_btree_page_t new_page;
    sakhadb_btree_node_t node = page->header;
    int rc = btreeLoadNewNode(tree->ctx, node->flags, &new_page);
    if(rc)
    {
        SLOG_BTREE_ERROR("btreeSplitNode: failed to load new node [%d]", rc);
        goto Lexit;
    }
    
    sakhadb_btree_node_t new_node = new_page->header;
    uint16_t k = node->nslots >> 1;
    
    btreeCopyOnSplit(node, new_node, k);
    new_node->right = node->right;
    
    register sakhadb_btree_slot_t* slot = btreeGetSlots(node);
    res->data = btreeNodeOffset(node, slot->off);
    res->size = slot->sz;
    res->new_page = new_page;
    
    // If node is leaf
    if(node->flags == SAKHADB_BTREE_LEAF)
    {
        node->right = new_page->no;
    }
    else
    {
        btreeRemoveLastSlot(node);
        node->right = slot->no;
    }
    
#ifdef DEBUG
    memset(btreeNodeOffset(node, node->free_off), 0, node->free_sz);
#endif
    
    btreeSaveNode(tree->ctx, page);
    btreeSaveNode(tree->ctx, new_page);
    
Lexit:
    return rc;
}

static inline int btreeSplitRoot(
    sakhadb_btree_t tree,
    sakhadb_btree_page_t* pLeftPage,
    sakhadb_btree_page_t* pRightPage
)
{
    SLOG_BTREE_INFO("btreeSplitRoot: split root [%d]", tree->root->no);
    sakhadb_btree_page_t left_page;
    sakhadb_btree_page_t right_page;
    sakhadb_btree_node_t root_node = tree->root->header;
    int rc = btreeLoadNewNode(tree->ctx, root_node->flags, &left_page);
    if(rc)
    {
        SLOG_BTREE_ERROR("btreeSplitRoot: failed to load new node [%d]", rc);
        goto Lexit;
    }
    
    rc = btreeLoadNewNode(tree->ctx, root_node->flags, &right_page);
    if(rc)
    {
        SLOG_BTREE_ERROR("btreeSplitRoot: failed to load new node [%d]", rc);
        goto Lexit;
    }
    
    int is_leaf = root_node->flags == SAKHADB_BTREE_LEAF;
    sakhadb_btree_node_t left_node = left_page->header;
    sakhadb_btree_node_t right_node = right_page->header;
    
    uint16_t k = root_node->nslots >> 1;
    sakhadb_btree_slot_t* base_slot = btreeGetSlots(root_node) + k;
    if(is_leaf)
    {
        btreeCopyOnSplit(root_node, right_node, k);
        btreeCopyOnSplit(root_node, left_node, root_node->nslots);
        
        assert(root_node->nslots == 0);
        assert(root_node->free_off == sizeof(struct BtreePageHeader));
        assert(root_node->slots_off == sakhadb_pager_page_size(tree->ctx->pager, 1));
        assert(root_node->free_sz == root_node->slots_off - root_node->free_off);
        
        btreeAppendSlot(root_node, base_slot)->no = left_page->no;
        
        left_node->right = right_page->no;
        right_node->right = root_node->right;
        root_node->right = right_page->no;
        
        // Root is not leaf anymore
        root_node->flags = 0;
    }
    else
    {
        btreeCopyOnSplit(root_node, right_node, k);
        btreeRemoveLastSlot(root_node);
        btreeCopyOnSplit(root_node, left_node, root_node->nslots);
        
        assert(root_node->nslots == 0);
        assert(root_node->free_off == sizeof(struct BtreePageHeader));
        assert(root_node->slots_off == sakhadb_pager_page_size(tree->ctx->pager, 1));
        assert(root_node->free_sz == root_node->slots_off - root_node->free_off);
        
        register sakhadb_btree_slot_t* new_slot = btreeAppendSlot(root_node, base_slot);
        left_node->right = new_slot->no;
        new_slot->no = left_page->no;
        
        right_node->right = root_node->right;
        root_node->right = right_page->no;
    }
    
    btreeSaveNode(tree->ctx, tree->root);
    btreeSaveNode(tree->ctx, left_page);
    btreeSaveNode(tree->ctx, right_page);
    
    *pLeftPage = left_page;
    *pRightPage = right_page;
    
Lexit:
    return rc;
}

/******************************************************************************/

/***************************** Insert Section *********************************/

static inline void btreeInsertInNode(
    struct BtreeCursorPointer * cursor,
    void* key, uint16_t nkey,
    Pgno no
)
{
    SLOG_BTREE_INFO("btreeInsertInNode: insert in node [%d][%d][%d]",
                    cursor->page->no, cursor->index, no);
    assert(cursor->page->header->free_sz >= nkey + sizeof(sakhadb_btree_node_t));
    
    register int idx = cursor->index;
    sakhadb_btree_node_t node = cursor->page->header;
    sakhadb_btree_slot_t* slots = btreeGetSlots(node);
    int is_leaf = node->flags;
    
    uint16_t off;
    register char* ptr;
    sakhadb_btree_slot_t* new_slot = slots + idx;
    if(idx == -1)
    {
        off = node->free_off;
        ptr = btreeNodeOffset(node, off);
        if(is_leaf)
        {
            new_slot->no = no;
        }
        else
        {
            new_slot->no = node->right;
            node->right = no;
        }
    }
    else
    {
        off = slots[idx].off;
        ptr = btreeNodeOffset(node, off);
        memmove(ptr + nkey, ptr, node->free_off - off);
        memmove(slots - 1, slots, (idx + 1) * sizeof(sakhadb_btree_slot_t));
        for(sakhadb_btree_slot_t *s = slots - 1; s != new_slot; ++s)
        {
            s->off += nkey;
        }
        
        if(is_leaf)
        {
            new_slot->no = no;
        }
        else
        {
            sakhadb_btree_slot_t* old_slot = new_slot - 1;
            new_slot->no = old_slot->no;
            old_slot->no = no;
        }
    }
    memcpy(ptr, key, nkey);
    new_slot->off = off;
    new_slot->sz = nkey;
    node->free_off += nkey;
    node->nslots += 1;
    node->slots_off -= sizeof(sakhadb_btree_slot_t);
    node->free_sz -= sizeof(sakhadb_btree_slot_t) + nkey;
}

static inline int btreeInsert(
    sakhadb_btree_t tree,
    void* key, uint16_t nkey,
    Pgno no
)
{
    SLOG_BTREE_INFO("btreeInsert: insert in tree [%d][%d]", tree->root->no, no);
    assert(nkey < sakhadb_pager_page_size(tree->ctx->pager, 0) / 5);
    int rc = SAKHADB_OK;
    struct BtreeCursorStack stack;
    rc = cpl_array_init(&stack.st, sizeof(struct BtreeCursorPointer), 16);
    if(rc)
    {
        SLOG_BTREE_ERROR("btreeInsert: failed to init cursors stack [%d]", rc);
        goto Lexit;
    }
    
    int cmp = btreeFind(tree, key, nkey, &stack);
    if(cmp == 0)
    {
        SLOG_BTREE_WARN("btreeInsert: keys duplicated");
        goto Ldexit;
    }
    
    struct BtreeCursorPointer* cur;
    while (cpl_array_count(&stack.st) > 1)
    {
        cur = (struct BtreeCursorPointer *)cpl_array_back_p(&stack.st);
        
        register sakhadb_btree_node_t node = cur->page->header;
        struct BtreeSplitResult res;
        if(node->free_sz < nkey + sizeof(sakhadb_btree_node_t))
        {
            rc = btreeSplitNode(tree, cur->page, &res);
            
            if(rc)
            {
                SLOG_BTREE_ERROR("btreeInsert: failed to split node [%d][%d]", rc, cur->page->no);
                goto Ldexit;
            }
            
            register sakhadb_btree_page_t new_page = res.new_page;
            if(cur->index < new_page->header->nslots)
            {
                cur->page = new_page;
            }
            else
            {
                cur->index -= new_page->header->nslots;
            }
            
            btreeInsertInNode(cur, key, nkey, no);
            
            key = res.data;
            nkey = res.size;
            no = new_page->no;
        }
        else
        {
            goto Linsertexit;
        }
        cpl_array_pop_back(&stack.st);
    }
    
    SLOG_BTREE_INFO("btreeInsert: inserting in root...");
    
    cur = (struct BtreeCursorPointer *)cpl_array_back_p(&stack.st);
    register sakhadb_btree_node_t node = cur->page->header;
    if(node->free_sz < nkey + sizeof(sakhadb_btree_node_t))
    {
        sakhadb_btree_page_t left_page, right_page;
        rc = btreeSplitRoot(tree, &left_page, &right_page);
        
        if(rc)
        {
            SLOG_BTREE_ERROR("btreeInsert: failed to split root [%d][%d]", rc, tree->root->no);
            goto Ldexit;
        }
        
        if(cur->index < right_page->header->nslots)
        {
            cur->page = right_page;
        }
        else
        {
            cur->page = left_page;
            cur->index -= right_page->header->nslots;
        }
    }
    
Linsertexit:
    btreeInsertInNode(cur, key, nkey, no);
    btreeSaveNode(tree->ctx, cur->page);
    
Ldexit:
    cpl_array_deinit(&stack.st);
    
Lexit:
    return rc;
}

/******************************************************************************/

/****************************** Cursor Section ********************************/

static inline int btreeCreateCursor(sakhadb_btree_cursor_t* pCursor)
{
    SLOG_BTREE_INFO("btreeDestroyCursor: create cursor");
    
    sakhadb_btree_cursor_t cursor = cpl_allocator_allocate(cpl_allocator_get_default(),
                                                           sizeof(struct BtreeCursorStack));
    
    if(!cursor)
    {
        SLOG_BTREE_FATAL("btreeCreateCursor: failed to allocate memory for cursor");
        return SAKHADB_NOMEM;
    }
    
    int rc = cpl_array_init(&cursor->st, sizeof(struct BtreeCursorPointer), 16);
    
    if(rc == SAKHADB_OK)
    {
        *pCursor = cursor;
        return SAKHADB_OK;
    }
    
    SLOG_BTREE_ERROR("btreeCreateCursor: failed to init stack for cursor");
    cpl_allocator_free(cpl_allocator_get_default(), cursor);
    return rc;
}

static inline void btreeDestroyCursor(sakhadb_btree_cursor_t __restrict cursor)
{
    SLOG_BTREE_INFO("btreeDestroyCursor: destroy cursor");
    cpl_array_deinit(&cursor->st);
    cpl_allocator_free(cpl_allocator_get_default(), cursor);
}

/******************************************************************************/

/***************************** Public Interface *******************************/
int sakhadb_btree_ctx_create(sakhadb_pager_t pager, sakhadb_btree_ctx_t* ctx)
{
    assert(ctx);
    
    SLOG_BTREE_INFO("sakhadb_btree_ctx_create: creating btree representation");
    cpl_allocator_ref default_allocator = cpl_allocator_get_default();
    struct BtreeContext* env = (struct BtreeContext *)cpl_allocator_allocate(default_allocator, sizeof(struct BtreeContext));
    if(!env)
    {
        SLOG_FATAL("sakhadb_btree_ctx_create: failed to allocate memory for struct Btree");
        return SAKHADB_NOMEM;
    }
    
    env->pager = pager;
    
    *ctx = env;
    return SAKHADB_OK;
}

void sakhadb_btree_ctx_destroy(sakhadb_btree_ctx_t ctx)
{
    assert(ctx);
    
    SLOG_BTREE_INFO("sakhadb_btree_ctx_destroy: destroying btree representation");
    cpl_allocator_free(cpl_allocator_get_default(), ctx);
}

int sakhadb_btree_create(sakhadb_btree_ctx_t ctx, Pgno no, sakhadb_btree_t* tree)
{
    assert(ctx);
    
    sakhadb_btree_page_t root;
    int rc = btreeLoadNode(ctx, no, &root);
    if(rc)
    {
        SLOG_FATAL("sakhadb_btree_create: failed to load Btree root. [%d]", rc);
        goto Lexit;
    }
    
//    sakhadb_btree_node_t node = root->header;
    
//    if(!node->free_sz)
//    {
//        node->nslots = 0;
//        node->slots_off = sakhadb_pager_page_size(env->pager, 1);
//        node->free_off = sizeof(struct BtreePageHeader);
//        node->free_sz = node->slots_off - sizeof(struct BtreePageHeader);
//        node->flags = SAKHADB_BTREE_LEAF;
//    }
    
    struct Btree* pTree;
    rc = btreeCreate(ctx, root, &pTree);
    if(rc)
    {
        SLOG_FATAL("sakhadb_btree_create: failed to create B-tree. [%d]", rc);
    }
    
Lexit:
    return rc;
}

void sakhadb_btree_destroy(sakhadb_btree_t tree)
{
    assert(tree);
    btreeDestroy(tree);
}

int sakhadb_btree_ctx_commit(sakhadb_btree_ctx_t ctx)
{
    assert(ctx);
    
    SLOG_BTREE_INFO("sakhadb_btree_ctx_commit: commit changes.");
    
    return sakhadb_pager_sync(ctx->pager);
}

int sakhadb_btree_ctx_rollback(sakhadb_btree_ctx_t ctx)
{
    assert(ctx);
    
    SLOG_BTREE_INFO("sakhadb_btree_ctx_rollback: rollback changes");
    
    return sakhadb_pager_update(ctx->pager);
}

int sakhadb_btree_insert(sakhadb_btree_t tree, void* key, size_t nkey, Pgno no)
{
    assert(tree && key && nkey && no);
    SLOG_BTREE_INFO("sakhadb_btree_insert: insert new element [%d][%d][%d]", tree->root->no, nkey, no);
    
    return btreeInsert(tree, key, nkey, no);
}

sakhadb_btree_cursor_t sakhadb_btree_find(sakhadb_btree_t tree, void* key, size_t nkey)
{
    assert(tree && key && nkey);
    SLOG_BTREE_INFO("sakhadb_btree_find: find key in tree [%d][%d]", tree->root->no, nkey);
    
    sakhadb_btree_cursor_t cursor = 0;
    int rc = btreeCreateCursor(&cursor);
    if(rc)
    {
        return 0;
    }
    
    int cmp = btreeFind(tree, key, nkey, cursor);
    
    if(cmp == 0)
    {
        cursor->tree = tree;
        return cursor;
    }
    
    btreeDestroyCursor(cursor);
    
Lexit:
    return 0;
}

void sakhadb_btree_cursor_destroy(sakhadb_btree_cursor_t cursor)
{
    btreeDestroyCursor(cursor);
}

Pgno sakhadb_btree_cursor_pgno(sakhadb_btree_cursor_t cursor)
{
    struct BtreeCursorPointer* ptr = (struct BtreeCursorPointer*)cpl_array_back_p(&cursor->st);
    return btreeGetDataPgno(ptr->page->header, ptr->index);
}

