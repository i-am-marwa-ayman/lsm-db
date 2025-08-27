package memtable

import (
	"bytes"
	"fmt"
)

type AVL struct {
	key    []byte
	val    *Entry
	height int
	right  *AVL
	left   *AVL
}

func NewAVL(key []byte, val *Entry) *AVL {
	return &AVL{
		key:    key,
		val:    val,
		height: 0,
		right:  nil,
		left:   nil,
	}
}

func (avl *AVL) childHeight(child *AVL) int {
	if child == nil {
		return -1
	}
	return child.height
}
func (avl *AVL) updateHeight() {
	avl.height = 1 + max(avl.childHeight(avl.left), avl.childHeight(avl.right))
}
func (avl *AVL) balanceFactor() int {
	return avl.childHeight(avl.left) - avl.childHeight(avl.right)
}
func rightRotation(q *AVL) *AVL {
	p := q.left
	q.left = p.right
	p.right = q

	q.updateHeight()
	p.updateHeight()

	return p
}
func leftRotation(p *AVL) *AVL {
	q := p.right
	p.right = q.left
	q.left = p

	p.updateHeight()
	q.updateHeight()

	return q
}
func balance(node *AVL) *AVL {
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

func (avl *AVL) Insert(key []byte, val *Entry) (*AVL, int) {
	newAdd := val.size()
	if avl == nil {
		avl = NewAVL(key, val)
	}
	if bytes.Equal(key, avl.key) {
		avl.val = val
		newAdd = 0
	} else if bytes.Compare(key, avl.key) < 0 {
		if avl.left == nil {
			avl.left = NewAVL(key, val)
		} else {
			avl.left, newAdd = avl.left.Insert(key, val)
		}
	} else {
		if avl.right == nil {
			avl.right = NewAVL(key, val)
		} else {
			avl.right, newAdd = avl.right.Insert(key, val)
		}
	}
	avl.updateHeight()
	return balance(avl), newAdd
}

func (avl *AVL) LookUp(key []byte) *Entry {
	if avl == nil {
		return nil
	}
	if bytes.Equal(key, avl.key) {
		return avl.val
	} else if bytes.Compare(key, avl.key) < 0 {
		return avl.left.LookUp(key)
	} else {
		return avl.right.LookUp(key)
	}
}

func (avl *AVL) GetAll() []*Entry {
	nodes := []*AVL{}

	top := avl

	entries := []*Entry{}

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
		entries = append(entries, top.val)
		top = top.right
	}
	return entries
}
func (avl *AVL) PrintAll() {
	if avl.left != nil {
		avl.left.PrintAll()
	}
	fmt.Printf("key: %s, val: %s, balance factor: %d\n", avl.key, avl.val.Value, avl.balanceFactor())
	if avl.right != nil {
		avl.right.PrintAll()
	}
}
