package lsm

import "fmt"

type avl struct {
	key    string
	val    *Entry
	height int
	right  *avl
	left   *avl
}

func NewAvl(key string, val *Entry) *avl {
	return &avl{
		key:    key,
		val:    val,
		height: 0,
		right:  nil,
		left:   nil,
	}
}

func (avl *avl) childHeight(child *avl) int {
	if child == nil {
		return -1
	}
	return child.height
}
func (avl *avl) updateHeight() {
	avl.height = 1 + max(avl.childHeight(avl.left), avl.childHeight(avl.right))
}
func (avl *avl) balanceFactor() int {
	return avl.childHeight(avl.left) - avl.childHeight(avl.right)
}
func rightRotation(q *avl) *avl {
	p := q.left
	q.left = p.right
	p.right = q

	q.updateHeight()
	p.updateHeight()

	return p
}
func leftRotation(q *avl) *avl {
	p := q.left
	q.left = p.right
	p.right = q

	q.updateHeight()
	p.updateHeight()

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

func (avl *avl) Insert(key string, val *Entry) *avl {
	if avl == nil {
		avl = NewAvl(key, val)
	}
	if key < avl.key {
		if avl.left == nil {
			avl.left = NewAvl(key, val)
		} else {
			avl.left = avl.left.Insert(key, val)
		}
	} else {
		if avl.right == nil {
			avl.right = NewAvl(key, val)
		} else {
			avl.right = avl.right.Insert(key, val)
		}
	}
	avl.updateHeight()
	return balance(avl)
}
func (avl *avl) GetAll() {
	if avl.left != nil {
		avl.left.GetAll()
	}
	fmt.Printf("key: %s, val: %s\n", avl.key, avl.val.Value)
	if avl.right != nil {
		avl.right.GetAll()
	}
}
