package memtable

import (
	"bytes"
	"fmt"

	"github.com/i-am-marwa-ayman/lsm-db/shared"
)

type avl struct {
	entry  *shared.Entry
	height int
	right  *avl
	left   *avl
}

func NewAVL(entry *shared.Entry) *avl {
	return &avl{
		entry:  entry,
		height: 0,
		right:  nil,
		left:   nil,
	}
}

func (node *avl) childHeight(child *avl) int {
	if child == nil {
		return -1
	}
	return child.height
}
func (node *avl) updateHeight() {
	node.height = 1 + max(node.childHeight(node.left), node.childHeight(node.right))
}
func (node *avl) balanceFactor() int {
	return node.childHeight(node.left) - node.childHeight(node.right)
}
func rightRotation(q *avl) *avl {
	p := q.left
	q.left = p.right
	p.right = q

	q.updateHeight()
	p.updateHeight()

	return p
}
func leftRotation(p *avl) *avl {
	q := p.right
	p.right = q.left
	q.left = p

	p.updateHeight()
	q.updateHeight()

	return q
}
func balance(node *avl) *avl {
	if node.balanceFactor() == 2 { // left-something
		if node.left.balanceFactor() == -1 { // left-right
			node.left = leftRotation(node.left) // convert to left-left
		}
		node = rightRotation(node) // convert to balanced
	}
	if node.balanceFactor() == -2 { // right-something
		if node.right.balanceFactor() == 1 { // right-left
			node.right = rightRotation(node.right) // convert to right-right
		}
		node = leftRotation(node) // convert to balanced
	}
	return node
}

func (node *avl) Insert(entry *shared.Entry) (*avl, int) {
	newAdd := entry.Size()
	if node == nil {
		node = NewAVL(entry)
	}
	if bytes.Equal(entry.Key, node.entry.Key) {
		newAdd = entry.Size() - node.entry.Size()
		node.entry = entry
	} else if bytes.Compare(entry.Key, node.entry.Key) < 0 {
		if node.left == nil {
			node.left = NewAVL(entry)
		} else {
			node.left, newAdd = node.left.Insert(entry)
		}
	} else {
		if node.right == nil {
			node.right = NewAVL(entry)
		} else {
			node.right, newAdd = node.right.Insert(entry)
		}
	}
	node.updateHeight()
	return balance(node), newAdd
}

func (node *avl) LookUp(key []byte) *shared.Entry {
	if node == nil {
		return nil
	}
	if bytes.Equal(key, node.entry.Key) {
		return node.entry
	} else if bytes.Compare(key, node.entry.Key) < 0 {
		return node.left.LookUp(key)
	} else {
		return node.right.LookUp(key)
	}
}

func (node *avl) GetAll() []*shared.Entry {
	nodes := []*avl{}

	top := node

	entries := []*shared.Entry{}

	//          1
	//        /   \
	//      2      3
	//    /  \
	//  4     5

	for len(nodes) > 0 || top != nil {
		for top != nil {
			nodes = append(nodes, top)
			top = top.left
		}
		top = nodes[len(nodes)-1] // 4
		nodes = nodes[:len(nodes)-1]
		entries = append(entries, top.entry)
		top = top.right
	}
	return entries
}
func (node *avl) PrintAll() {
	if node.left != nil {
		node.left.PrintAll()
	}
	fmt.Printf("key: %s, val: %s, balance factor: %d\n", node.entry.Key, node.entry.Value, node.balanceFactor())
	if node.right != nil {
		node.right.PrintAll()
	}
}
