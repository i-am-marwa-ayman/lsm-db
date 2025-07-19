package lsm

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
func (avl *avl) Insert(key string, val *Entry) {
	if avl == nil {
		avl = NewAvl(key, val)
	}
	if key < avl.key {
		if avl.left == nil {
			avl.left = NewAvl(key, val)
		} else {
			avl.left.Insert(key, val)
		}
	} else {
		if avl.right == nil {
			avl.right = NewAvl(key, val)
		} else {
			avl.right.Insert(key, val)
		}
	}
	avl.updateHeight()
}
