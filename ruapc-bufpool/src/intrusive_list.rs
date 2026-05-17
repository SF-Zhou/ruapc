//! Intrusive doubly-linked list for O(1) free list management.

use std::ptr::NonNull;

/// A node in an intrusive doubly-linked list.
#[derive(Debug)]
pub struct IntrusiveNode<T> {
    pub(crate) prev: Option<NonNull<Self>>,
    pub(crate) next: Option<NonNull<Self>>,
    pub(crate) data: T,
}

impl<T> IntrusiveNode<T> {
    /// Creates a new intrusive node with the given data.
    pub const fn new(data: T) -> Self {
        Self {
            prev: None,
            next: None,
            data,
        }
    }

    /// Returns `true` if this node is linked into a list.
    pub const fn is_linked(&self) -> bool {
        self.prev.is_some() || self.next.is_some()
    }
}

/// An intrusive doubly-linked list.
///
/// This list does not own its nodes - they must be allocated elsewhere.
/// The list only maintains pointers to the head and tail.
///
/// # Safety
///
/// The caller must ensure that:
/// - Nodes are not dropped while linked in the list
/// - Nodes are only linked in one list at a time
/// - Pointers remain valid for the lifetime of the list
pub struct IntrusiveList<T> {
    head: Option<NonNull<IntrusiveNode<T>>>,
    tail: Option<NonNull<IntrusiveNode<T>>>,
    len: usize,
}

impl<T> Default for IntrusiveList<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> IntrusiveList<T> {
    /// Creates a new empty intrusive list.
    pub const fn new() -> Self {
        Self {
            head: None,
            tail: None,
            len: 0,
        }
    }

    /// Returns the number of nodes in the list.
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if the list is empty.
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Pushes a node to the front of the list.
    ///
    /// # Safety
    ///
    /// The caller must ensure:
    /// - `node` points to a valid, unlinked `IntrusiveNode`
    /// - `node` will not be dropped while linked
    /// - `node` is not already in any list
    pub unsafe fn push_front(&mut self, node: NonNull<IntrusiveNode<T>>) {
        let node_ref = unsafe { node.as_ptr().as_mut().unwrap() };
        debug_assert!(!node_ref.is_linked(), "node is already linked");

        node_ref.prev = None;
        node_ref.next = self.head;

        if let Some(old_head) = self.head {
            unsafe {
                (*old_head.as_ptr()).prev = Some(node);
            }
        } else {
            self.tail = Some(node);
        }

        self.head = Some(node);
        self.len += 1;
    }

    /// Pops a node from the front of the list.
    pub fn pop_front(&mut self) -> Option<NonNull<IntrusiveNode<T>>> {
        let head = self.head?;

        unsafe {
            let head_ref = head.as_ptr().as_mut().unwrap();
            self.head = head_ref.next;

            if let Some(new_head) = self.head {
                (*new_head.as_ptr()).prev = None;
            } else {
                self.tail = None;
            }

            head_ref.prev = None;
            head_ref.next = None;
        }

        self.len -= 1;
        Some(head)
    }

    /// Removes a specific node from the list in O(1).
    ///
    /// # Safety
    ///
    /// The caller must ensure:
    /// - `node` points to a valid `IntrusiveNode` that is currently in this list
    pub unsafe fn remove(&mut self, node: NonNull<IntrusiveNode<T>>) {
        let node_ref = unsafe { node.as_ptr().as_mut().unwrap() };

        match (node_ref.prev, node_ref.next) {
            (Some(prev), Some(next)) => unsafe {
                (*prev.as_ptr()).next = Some(next);
                (*next.as_ptr()).prev = Some(prev);
            },
            (None, Some(next)) => {
                unsafe {
                    (*next.as_ptr()).prev = None;
                }
                self.head = Some(next);
            }
            (Some(prev), None) => {
                unsafe {
                    (*prev.as_ptr()).next = None;
                }
                self.tail = Some(prev);
            }
            (None, None) => {
                self.head = None;
                self.tail = None;
            }
        }

        node_ref.prev = None;
        node_ref.next = None;
        self.len -= 1;
    }
}

// SAFETY: The list only contains raw pointers which are Send if T is Send
unsafe impl<T: Send> Send for IntrusiveList<T> {}

// SAFETY: The list only contains raw pointers which are Sync if T is Sync
unsafe impl<T: Sync> Sync for IntrusiveList<T> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_list() {
        let list: IntrusiveList<i32> = IntrusiveList::new();
        assert!(list.is_empty());
        assert_eq!(list.len(), 0);
    }

    #[test]
    fn test_push_pop_single() {
        let mut list: IntrusiveList<i32> = IntrusiveList::new();
        let mut node = IntrusiveNode::new(42);
        let node_ptr = NonNull::new(&raw mut node).unwrap();

        unsafe {
            list.push_front(node_ptr);
        }

        assert!(!list.is_empty());
        assert_eq!(list.len(), 1);

        let popped = list.pop_front().unwrap();
        unsafe {
            assert_eq!((*popped.as_ptr()).data, 42);
        }

        assert!(list.is_empty());
    }

    #[test]
    fn test_push_pop_multiple() {
        let mut list: IntrusiveList<i32> = IntrusiveList::new();
        let mut nodes = [
            IntrusiveNode::new(1),
            IntrusiveNode::new(2),
            IntrusiveNode::new(3),
        ];

        unsafe {
            list.push_front(NonNull::new(&raw mut nodes[0]).unwrap());
            list.push_front(NonNull::new(&raw mut nodes[1]).unwrap());
            list.push_front(NonNull::new(&raw mut nodes[2]).unwrap());
        }

        assert_eq!(list.len(), 3);

        // Should pop in reverse order (LIFO)
        unsafe {
            assert_eq!((*list.pop_front().unwrap().as_ptr()).data, 3);
            assert_eq!((*list.pop_front().unwrap().as_ptr()).data, 2);
            assert_eq!((*list.pop_front().unwrap().as_ptr()).data, 1);
        }

        assert!(list.is_empty());
    }

    #[test]
    fn test_remove_middle() {
        let mut list: IntrusiveList<i32> = IntrusiveList::new();
        let mut nodes = [
            IntrusiveNode::new(1),
            IntrusiveNode::new(2),
            IntrusiveNode::new(3),
        ];

        let ptr0 = NonNull::new(&raw mut nodes[0]).unwrap();
        let ptr1 = NonNull::new(&raw mut nodes[1]).unwrap();
        let ptr2 = NonNull::new(&raw mut nodes[2]).unwrap();

        unsafe {
            list.push_front(ptr0);
            list.push_front(ptr1);
            list.push_front(ptr2);

            list.remove(ptr1);
        }

        assert_eq!(list.len(), 2);

        unsafe {
            assert_eq!((*list.pop_front().unwrap().as_ptr()).data, 3);
            assert_eq!((*list.pop_front().unwrap().as_ptr()).data, 1);
        }
    }

    #[test]
    fn test_remove_head() {
        let mut list: IntrusiveList<i32> = IntrusiveList::new();
        let mut nodes = [IntrusiveNode::new(1), IntrusiveNode::new(2)];

        let ptr0 = NonNull::new(&raw mut nodes[0]).unwrap();
        let ptr1 = NonNull::new(&raw mut nodes[1]).unwrap();

        unsafe {
            list.push_front(ptr0);
            list.push_front(ptr1);

            list.remove(ptr1);
        }

        assert_eq!(list.len(), 1);

        unsafe {
            assert_eq!((*list.pop_front().unwrap().as_ptr()).data, 1);
        }
    }

    #[test]
    fn test_remove_tail() {
        let mut list: IntrusiveList<i32> = IntrusiveList::new();
        let mut nodes = [IntrusiveNode::new(1), IntrusiveNode::new(2)];

        let ptr0 = NonNull::new(&raw mut nodes[0]).unwrap();
        let ptr1 = NonNull::new(&raw mut nodes[1]).unwrap();

        unsafe {
            list.push_front(ptr0);
            list.push_front(ptr1);

            list.remove(ptr0);
        }

        assert_eq!(list.len(), 1);

        unsafe {
            assert_eq!((*list.pop_front().unwrap().as_ptr()).data, 2);
        }
    }
}
