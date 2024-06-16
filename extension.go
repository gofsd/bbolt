package bbolt

import (
	"bytes"
	"fmt"
	"sort"

	"go.etcd.io/bbolt/internal/common"
)

type TreeElementsComparer func(highest *[]byte, k, v []byte) (right bool, exact bool)

func (c *Cursor) SeekReverse(seek []byte) (key, value []byte) {
	lastKey, _ := c.Last()
	lkLength := len(lastKey)
	var highest = make([]byte, lkLength)
	copy(highest, seek)
	if len(seek) < lkLength {
		for i := range lkLength {
			if len(seek) <= i {
				highest[i] = 255
			}
		}
	}
	key, value = c.SeekCustom(&highest, CompareTreeElements)
	return
}

func (c *Cursor) SeekCustom(highest *[]byte, f TreeElementsComparer) (key, value []byte) {
	common.Assert(c.bucket.tx.db != nil, "tx closed")

	k, v, flags := c.seekCustom(highest, f)

	// If we ended up after the last element of a page then move to the next one.
	if ref := &c.stack[len(c.stack)-1]; ref.index >= ref.count() {
		k, v, flags = c.next()
	}

	if k == nil {
		return nil, nil
	} else if (flags & uint32(common.BucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

func (c *Cursor) seekCustom(highest *[]byte, f TreeElementsComparer) (key []byte, value []byte, flags uint32) {
	// Start from root page/node and traverse to correct page.
	c.stack = c.stack[:0]
	c.searchCustom(highest, c.bucket.RootPage(), f)

	// If this is a bucket then return a nil value.
	return c.keyValue()
}

func (c *Cursor) searchCustom(highest *[]byte, pgId common.Pgid, f TreeElementsComparer) {
	p, n := c.bucket.pageNode(pgId)
	if p != nil && !p.IsBranchPage() && !p.IsLeafPage() {
		panic(fmt.Sprintf("invalid page type: %d: %x", p.Id(), p.Flags()))
	}
	e := elemRef{page: p, node: n}
	c.stack = append(c.stack, e)

	// If we're on a leaf page/node then find the specific node.
	if e.isLeaf() {
		c.nsearchCustom(highest, f)
		return
	}

	if n != nil {
		c.searchNodeCustom(highest, n, f)
		return
	}
	c.searchPageCustom(highest, p, f)
}

// nsearch searches the leaf node on the top of the stack for a key.
func (c *Cursor) nsearchCustom(highest *[]byte, f TreeElementsComparer) {
	e := &c.stack[len(c.stack)-1]
	p, n := e.page, e.node

	// If we have a node then search its inodes.
	if n != nil {
		index := sort.Search(len(n.inodes), func(i int) bool {
			right, _ := f(highest, n.inodes[i].Key(), n.inodes[i].Value())
			return right
		})
		e.index = index
		return
	}

	// If we have a page then search its leaf elements.
	inodes := p.LeafPageElements()
	index := sort.Search(int(p.Count()), func(i int) bool {
		right, _ := f(highest, inodes[i].Key(), inodes[i].Value())
		return right
	})
	e.index = index
}

func (c *Cursor) searchNodeCustom(highest *[]byte, n *node, f TreeElementsComparer) {
	var (
		exact bool
	)

	index := sort.Search(len(n.inodes), func(i int) bool {
		// TODO(benbjohnson): Optimize this range search. It's a bit hacky right now.
		// sort.Search() finds the lowest index where f() != -1 but we need the highest index.
		var right bool
		right, exact = f(highest, n.inodes[i].Key(), n.inodes[i].Value())

		return right
	})
	if !exact && index > 0 {
		index--
	}
	c.stack[len(c.stack)-1].index = index

	// Recursively search to the next page.
	c.searchCustom(highest, n.inodes[index].Pgid(), f)
}

func (c *Cursor) searchPageCustom(highest *[]byte, p *common.Page, f TreeElementsComparer) {
	// Binary search for the correct range.
	inodes := p.BranchPageElements()

	var (
		exact bool
		index int
	)

	index = sort.Search(int(p.Count()), func(i int) bool {
		// TODO(benbjohnson): Optimize this range search. It's a bit hacky right now.
		// sort.Search() finds the lowest index where f() != -1 but we need the highest index.
		k := inodes[i].Key()

		var right bool
		right, exact = f(highest, k, []byte{})

		return right
	})
	if !exact && index > 0 {
		index--
	}
	c.stack[len(c.stack)-1].index = index

	// Recursively search to the next page.
	c.searchCustom(highest, inodes[index].Pgid(), f)
}

func CompareTreeElements(highest *[]byte, k, v []byte) (right bool, exact bool) {
	if bytes.Compare(k, *highest) >= 0 {
		return true, false
	} else if bytes.Compare(k, *highest) == 0 {
		return true, true
	} else {
		return false, false
	}
}

func (c *Cursor) SeekReverseSlow(prefix []byte) ([]byte, []byte) {
	var K, V []byte
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		K, V = k, v
	}

	return K, V
}
