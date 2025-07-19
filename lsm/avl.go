package lsm

type avl struct {
	key    string
	val    string
	height int
	right  *avl
	left   *avl
}

func NewAvl(key string, val string) *avl {
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
func (avl *avl) Insert(key string, val string) {
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
