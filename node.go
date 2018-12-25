package btree

import (
	"sort"
	"bytes"
)

type Btree struct {
	root *Node
}

func (b *Btree) Del(key []byte) {
	exist := false
	node := searchNode(key, b.root, &exist)
	if !exist {
		return
	}

	node.del(key)
	node.rebalance()
}

func (b *Btree) Insert(key, value []byte) {
	node := searchNode(key, b.root, nil)

	if len(node.items) == 0 {
		node.items.insertAt(0, &inode{key, value})
		return
	}

	index, found := node.items.search(key)
	if found {
		node.items[index].value = value
		return
	} else {
		node.items = append(node.items, &inode{})
		copy(node.items[index+1:], node.items[index:])
		node.items[index] = &inode{key, value}
	}

	// 判断是否需要分裂
	if len(node.items) > 4 {
		node.split(len(node.children) / 2)
	}
}

type Node struct {
	isLeaf   bool
	children Nodes
	parent   *Node
	key      []byte
	items    inodes
}

func (n *Node) rebalance() {
	if len(n.items) > n.minkey() {
		return
	}

	// 将子节点上移动
	if n.parent == nil {
		if !n.isLeaf && len(n.items) == 1 {
			child := n.children[0]
			n.isLeaf = child.isLeaf
			n.items = child.items[:]
			n.children = child.children

			for _, cc := range n.children {
				cc.parent = n
			}
			child.parent = nil
		}
		return
	}

	// 直接删除
	if len(n.items) == 0 {
		n.parent.del(n.key)
		n.parent.removeChild(n)
		n.parent.rebalance()
		return
	}

	var target *Node
	var rightmerged = (n.parent.childIndex(n) == 0)
	if rightmerged {
		target = n.nextSibling()
	} else {
		target = n.prevSibling()
	}

	// 右侧子节点合并
	if rightmerged {
		if !n.isLeaf {
			n.children = append(n.children, target.children...)
			for _, child := range target.children {
				child.parent.removeChild(child)
				child.parent = n
			}
		}
		n.items = append(n.items, target.items...)
		n.parent.del(target.key)
		n.parent.removeChild(target)
	} else {
		if !target.isLeaf {
			target.children = append(target.children, n.children...)
			for _, child := range n.children {
				child.parent.removeChild(child)
				child.parent = target
			}
		}
		target.items = append(target.items, n.items...)
		n.parent.del(n.key)
		n.parent.removeChild(n)
	}

	n.parent.rebalance()
}

func (n *Node) removeChild(target *Node) {
	for i, child := range n.children {
		if child == target {
			n.children = append(n.children[:i], n.children[i+1:]...)
			return
		}
	}
}

func (n *Node) nextSibling() *Node {
	if n.parent == nil {
		return nil
	}
	index := n.parent.childIndex(n)
	if index >= len(n.parent.children)-1 {
		return nil
	}
	return n.parent.children[index+1]
}

func (n *Node) prevSibling() *Node {
	if n.parent == nil {
		return nil
	}
	index := n.parent.childIndex(n)
	if index == 0 {
		return nil
	}
	return n.parent.children[index-1]
}

func (n *Node) childIndex(child *Node) int {
	index := sort.Search(len(n.items), func(i int) bool { return bytes.Compare(n.items[i].key, child.key) != -1 })
	return index
}

func (n *Node) minkey() int {
	if n.isLeaf {
		return 1
	}
	return 2
}

func (n *Node) del(key []byte) {
	// Find index of key.
	index := sort.Search(len(n.items), func(i int) bool { return bytes.Compare(n.items[i].key, key) != -1 })

	if index >= len(n.items) || !bytes.Equal(n.items[index].key, key) {
		return
	}

	n.items = append(n.items[:index], n.items[index+1:]...)
}

func (n *Node) split(splitIndex int) {
	if n.parent == nil {
		n.parent = &Node{children: []*Node{n}}
	}

	next := &Node{isLeaf: n.isLeaf, parent: n.parent}
	next.parent.children = append(next.parent.children, next)
	next.items = n.items[splitIndex:]
	n.items = n.items[:splitIndex]

	if len(n.children) > 0 {
		for i := splitIndex; i < len(n.children); i ++ {
			n.children[i].parent = next
			next.children = append(next.children, n.children[i])
			n.children = append(n.children[:i], n.children[i+1:]...)
		}

		n.children = n.children[:splitIndex]
	}

	if n.parent != nil && len(n.parent.children) > 5 {
		n.parent.split(len(n.parent.children) / 2)
	}
}

type Nodes []*Node

type inode struct {
	key   []byte
	value []byte
}

type inodes []*inode

var nilChildren = make(inodes, 16)

func (n *inodes) truncate(index int) {
	var toClear inodes
	*n, toClear = (*n)[:index], (*n)[index:]
	for len(toClear) > 0 {
		toClear = toClear[copy(toClear, nilChildren):]
	}
}

func (n *inodes) search(k []byte) (int, bool) {
	index := sort.Search(len(*n), func(i int) bool { return bytes.Compare((*n)[i].key, k) != -1 })
	if len(*n) > 0 && index < len(*n) && bytes.Equal((*n)[index].key, k) {
		return index, true
	}
	return index - 1, false
}

func (n *inodes) insertAt(index int, item *inode) {
	*n = append(*n, nil)
	if index < len(*n) {
		copy((*n)[index+1:], (*n)[index:])
	}
	(*n)[index] = item
}

func searchNode(key []byte, n *Node, exist *bool) *Node {
	if n.isLeaf {
		return n
	}

	var exact bool
	index := sort.Search(len(n.items), func(i int) bool {
		ret := bytes.Compare(n.items[i].key, key)
		if ret == 0 {
			*exist = true
			exact = true
		}
		return ret != -1
	})
	if !exact && index > 0 {
		index--
	}

	return searchNode(key, n.children[index], exist)
}
