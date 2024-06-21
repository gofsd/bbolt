package bbolt

import (
	"bytes"
	"fmt"

	"go.etcd.io/bbolt/internal/common"
)

type TreeElementsComparer func(k, v []byte) (right, exact, hasPrefix bool)
type Search func(flag uint8, n int, f func(int) (bool, bool)) int

func (c *Cursor) SeekReverse(seek []byte) (key, value []byte) {

	var maxValue, saved, highest []byte
	saved = append(saved, seek...)
	maxValue = append(maxValue, seek...)
	highest = append(highest, seek...)
	if maxValue[len(maxValue)-1] == 255 {
		var shouldIncreasePlace = true
		for i := len(maxValue) - 1; i >= 0; i-- {
			if maxValue[i] < 255 {
				maxValue[i] = maxValue[i] + 1
				shouldIncreasePlace = false
				break
			}
		}
		if shouldIncreasePlace {
			maxValue = append(maxValue, 0)
		}
	} else {
		maxValue[len(maxValue)-1] = maxValue[len(maxValue)-1] + 1
	}

	cp := func(k, v []byte) (right, exact, hasPrefix bool) {
		if bytes.Compare(k, seek) > 0 {
			if bytes.Compare(k, maxValue) < 0 {
				if bytes.HasPrefix(k, saved) {
					seek = k
					hasPrefix = true
					if len(highest) < len(k) {
						for i := range len(k) {
							if len(highest) <= i {
								highest = append(highest, 255)
							}
						}
					}
					if bytes.Compare(k, highest) == 0 {
						return true, true, true
					}
				}

				return false, false, hasPrefix
			}
			return true, false, hasPrefix
		} else if bytes.Compare(k, seek) == 0 {
			return true, true, hasPrefix
		} else {
			return false, false, hasPrefix
		}
	}
	s := func(flag uint8, n int, f func(int) (right, hasPrefix bool)) int {
		// Define f(-1) == false and f(n) == true.
		// Invariant: f(i-1) == false, f(j) == true.
		i, j, b := 0, n, 0
		var hasPrefix, right bool
		for i < j {
			h := int(uint(i+j) >> 1) // avoid overflow when computing h
			// i ≤ h < j
			right, hasPrefix = f(h)
			if hasPrefix {
				b = h
			}
			if !right {
				i = h + 1 // preserves f(i-1) == false
			} else {
				j = h // preserves f(j) == true
			}
		}
		if flag == common.LeafPageFlag && i == n && hasPrefix {
			return b
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
		index := s(common.LeafPageFlag, len(n.inodes), func(i int) (right, hasPrefix bool) {
			right, _, hasPrefix = f(n.inodes[i].Key(), n.inodes[i].Value())
			return right, hasPrefix
		})
		e.index = index
		return
	}

	// If we have a page then search its leaf elements.
	inodes := p.LeafPageElements()
	index := s(common.LeafPageFlag, int(p.Count()), func(i int) (right, hasPrefix bool) {
		right, _, hasPrefix = f(inodes[i].Key(), inodes[i].Value())
		return right, hasPrefix
	})
	e.index = index
}

func (c *Cursor) searchNodeCustom(n *node, s Search, f TreeElementsComparer) {
	var (
		exact bool
	)

	index := s(0, len(n.inodes), func(i int) (right, hasPrefix bool) {
		// TODO(benbjohnson): Optimize this range search. It's a bit hacky right now.
		// sort.Search() finds the lowest index where f() != -1 but we need the highest index.
		right, exact, hasPrefix = f(n.inodes[i].Key(), n.inodes[i].Value())

		return right, hasPrefix
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

	index = s(0, int(p.Count()), func(i int) (right, hasPrefix bool) {
		// TODO(benbjohnson): Optimize this range search. It's a bit hacky right now.
		// sort.Search() finds the lowest index where f() != -1 but we need the highest index.
		k := inodes[i].Key()

		right, exact, hasPrefix = f(k, []byte{})

		return right, hasPrefix
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
