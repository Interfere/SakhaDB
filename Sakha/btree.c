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

enum
{
    KEY_SMALLEST = -1,
    KEY_FOUND    = 0,
    KEY_BIGGER   = 1
};

/***************************** Private Interface ******************************/
typedef struct BtreeSlot slot_t;
struct BtreeSlot
{
    uint16_t    off;        /* Offset to the key */
    uint16_t    sz;         /* Size of the key */
};

struct BtreeData {
    Pgno        overflow;   /* Next overflow page */
    int32_t     size;       /* length of data in this page */
    char        data[256];
};

int btreeLoadNode(Pgno no, struct BtreePageHeader** pNode)
{
    // TODO: add loading page
    return SAKHADB_OK;
}

int btreeFindKey(struct BtreePageHeader* node, void* key, uint16_t key_sz, slot_t** pRes)
{
    if(node->nslots == 0)
    {
        return KEY_SMALLEST;
    }
    
    slot_t* slots = (slot_t*)((char*)node + node->slots_off);
    int lower_bound = 0;
    int upper_bound = node->nslots - 1;
    int middle = upper_bound / 2;
    int ret;
    while (upper_bound > lower_bound) {
        slot_t* slot = slots + middle;
        char* stored_key = (char*)node + slot->off;
        int res = memcmp(key, stored_key, (slot->off > key_sz)?slot->off:key_sz);
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

/***************************** Public Interface *******************************/
int sakhadb_btree_find_key(sakhadb_btree_node_t root, void* key, size_t sz)
{
    int ret = SAKHADB_OK;
    sakhadb_btree_node_t node = root;
    int is_leaf = root->free_sz & 1;
    int is_index = root->free_sz & 2;
    while (1) {
        slot_t* slot;
        int res = btreeFindKey(node, key, sz, &slot);
        switch (res) {
            case KEY_FOUND:
                if((!is_index) || (is_index && is_leaf))
                {
                    goto Lexit;
                }
            case KEY_BIGGER:
                if(!is_leaf)
                {
                    Pgno* pNo = (Pgno *)((char*)node + slot->off + slot->sz);
                    ret = btreeLoadNode(*pNo, &node);
                    if(ret != SAKHADB_OK)
                    {
                        goto Lexit;
                    }
                    break;
                }
                ret = SAKHADB_NOTFOUND;
                goto Lexit;
                
            case KEY_SMALLEST:
                if(!is_leaf)
                {
                    ret = btreeLoadNode(node->next, &node);
                    if(ret != SAKHADB_OK)
                    {
                        goto Lexit;
                    }
                    break;
                }
                ret = SAKHADB_NOTFOUND;
                goto Lexit;
                
            default:
                assert(0);
                ret = SAKHADB_NOTFOUND;
                goto Lexit;
        }
    }
    
Lexit:
    return ret;
}

