package bbolt

import (
	"bytes"
	"fmt"

	"go.etcd.io/bbolt/internal/common"
)

type TreeElementsComparer func(k, v []byte) (right bool, exact bool)
type Search func(n int, f func(int) bool) int

func (c *Cursor) SeekReverse(seek []byte) (key, value []byte) {

	var maxValue, saved []byte
	saved = append(saved, seek...)
	l := len(seek)
	maxValue = append(maxValue, seek...)
	maxValue[l-1] = maxValue[l-1] + 1
	lastKey, _ := c.Last()
	lkLength := len(lastKey)
	var highest = make([]byte, lkLength)
	copy(highest, saved)
	if len(seek) < lkLength {
		for i := range lkLength {
			if len(seek) <= i {
				highest[i] = 255
			}
		}
	}
	cp := func(k, v []byte) (right bool, exact bool) {
		if bytes.Compare(k, seek) > 0 {
			if bytes.Compare(k, seek) > 0 && bytes.Compare(k, maxValue) < 0 {
				if bytes.HasPrefix(k, saved) {
					seek = k
				}
				if bytes.Compare(k, highest) == 0 {
					return true, true
				}
				return false, false
			}
			return true, false
		} else if bytes.Compare(k, seek) == 0 {
			return true, true
		} else {
			return false, false
		}
	}
	s := func(n int, f func(int) bool) int {
		// Define f(-1) == false and f(n) == true.
		// Invariant: f(i-1) == false, f(j) == true.
		i, j := 0, n
		for i < j {
			h := int(uint(i+j) >> 1) // avoid overflow when computing h
			// i ≤ h < j
			if !f(h) {
				i = h + 1 // preserves f(i-1) == false
			} else {
				j = h // preserves f(j) == true
			}
		}

		// i == j, f(i-1) == false, and f(j) (= f(i)) == true  =>  answer is i.
		return i
	}
	key, value = c.SeekCustom(s, cp)
	return
}

func (c *Cursor) SeekCustom(s Search, f TreeElementsComparer) (key, value []byte) {
	common.Assert(c.bucket.tx.db != nil, "tx closed")

	k, v, flags := c.seekCustom(s, f)

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

func (c *Cursor) seekCustom(s Search, f TreeElementsComparer) (key []byte, value []byte, flags uint32) {
	// Start from root page/node and traverse to correct page.
	c.stack = c.stack[:0]
	c.searchCustom(c.bucket.RootPage(), s, f)

	// If this is a bucket then return a nil value.
	return c.keyValue()
}

func (c *Cursor) searchCustom(pgId common.Pgid, s Search, f TreeElementsComparer) {
	p, n := c.bucket.pageNode(pgId)
	if p != nil && !p.IsBranchPage() && !p.IsLeafPage() {
		panic(fmt.Sprintf("invalid page type: %d: %x", p.Id(), p.Flags()))
	}
	e := elemRef{page: p, node: n}
	c.stack = append(c.stack, e)

	// If we're on a leaf page/node then find the specific node.
	if e.isLeaf() {
		c.nsearchCustom(s, f)
		return
	}

	if n != nil {
		c.searchNodeCustom(n, s, f)
		return
	}
	c.searchPageCustom(p, s, f)
}

// nsearch searches the leaf node on the top of the stack for a key.
func (c *Cursor) nsearchCustom(s Search, f TreeElementsComparer) {
	e := &c.stack[len(c.stack)-1]
	p, n := e.page, e.node

	// If we have a node then search its inodes.
	if n != nil {
		index := s(len(n.inodes), func(i int) bool {
			right, _ := f(n.inodes[i].Key(), n.inodes[i].Value())
			return right
		})
		e.index = index
		return
	}

	// If we have a page then search its leaf elements.
	inodes := p.LeafPageElements()
	index := s(int(p.Count()), func(i int) bool {
		right, _ := f(inodes[i].Key(), inodes[i].Value())
		return right
	})
	e.index = index
}

func (c *Cursor) searchNodeCustom(n *node, s Search, f TreeElementsComparer) {
	var (
		exact bool
	)

	index := s(len(n.inodes), func(i int) bool {
		// TODO(benbjohnson): Optimize this range search. It's a bit hacky right now.
		// sort.Search() finds the lowest index where f() != -1 but we need the highest index.
		var right bool
		right, exact = f(n.inodes[i].Key(), n.inodes[i].Value())

		return right
	})
	if !exact && index > 0 {
		index--
	}
	c.stack[len(c.stack)-1].index = index

	// Recursively search to the next page.
	c.searchCustom(n.inodes[index].Pgid(), s, f)
}

func (c *Cursor) searchPageCustom(p *common.Page, s Search, f TreeElementsComparer) {
	// Binary search for the correct range.
	inodes := p.BranchPageElements()

	var (
		exact bool
		index int
	)

	index = s(int(p.Count()), func(i int) bool {
		// TODO(benbjohnson): Optimize this range search. It's a bit hacky right now.
		// sort.Search() finds the lowest index where f() != -1 but we need the highest index.
		k := inodes[i].Key()

		var right bool
		right, exact = f(k, []byte{})

		return right
	})
	if !exact && index > 0 {
		index--
	}
	c.stack[len(c.stack)-1].index = index

	// Recursively search to the next page.
	c.searchCustom(inodes[index].Pgid(), s, f)
}

func (c *Cursor) SeekReverseSlow(prefix []byte) ([]byte, []byte) {
	var K, V []byte
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		K, V = k, v
	}

	return K, V
}
