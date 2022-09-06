use std::{mem::MaybeUninit, sync::atomic::{AtomicUsize, Ordering}, cell::UnsafeCell};

/// A lock-free version of `Vec`.
#[derive(Debug)]
struct Vector<T, const BLOCK_SIZE: usize = 4096> {

  blocks: Vec<Box<Block<T, BLOCK_SIZE>>>,
  /// Number of elements
  size: AtomicUsize,
  /// Position of the place where elements can be added (may differ from size because there may be
  /// multiple concurrent ongoing writes)
  head: AtomicUsize,
  /// Number of allocated blocks × BLOCK_SIZE
  capacity: usize,
}

#[derive(Debug)]
struct Block<T, const BLOCK_SIZE: usize = 4096> {
  data: [UnsafeCell<MaybeUninit<T>>; BLOCK_SIZE],
}

impl<T, const BLOCK_SIZE: usize> Vector<T, BLOCK_SIZE> {
  const MASK: usize = BLOCK_SIZE - 1;
  const SHIFT: usize = {
    let mut n = 0;
    let mut x = BLOCK_SIZE;
    while x != 0 {
      n += 1;
      x >>= 1;
    };
    n
  };

  const EMPTY_ELEM: UnsafeCell<MaybeUninit<T>> = UnsafeCell::new(MaybeUninit::uninit());
  const EMPTY_BLOCK: Block<T, BLOCK_SIZE> = Block {
    data: [Self::EMPTY_ELEM; BLOCK_SIZE],
  };

  /// Creates an empty vector, with no allocated blocks.
  pub fn new() -> Self {
    Self {
      blocks: Vec::new(),
      size: AtomicUsize::new(0),
      head: AtomicUsize::new(0),
      capacity: 0,
    }
  }

  /// Creates an empty vector, with some pre-allocated capacity
  pub fn new_with_capacity(capacity: usize) -> Self {
    let n_blocks = (capacity >> Self::SHIFT) + 1;
    let blocks = 
      (0..n_blocks)
        .map(|_| {
          let data = [Self::EMPTY_ELEM; BLOCK_SIZE];
          Box::new(Block { data })
        })
        .collect();
    Self {
      blocks: blocks,
      size: AtomicUsize::new(0),
      head: AtomicUsize::new(0),
      capacity: capacity,
    }
  }

  /// Read value at `idx`.
  pub fn read(&self, idx: usize) -> Option<&T> {
    // In this vector we never de-allocate existing blocks, so reading is a very simple matter
    if idx <= self.size.load(Ordering::Acquire) {
      let big_idx = idx >> Self::SHIFT;
      let small_idx = idx & Self::MASK;
      let ptr = self.blocks[big_idx].data[small_idx].get() as *const T;
      Some(unsafe { &*ptr })
    } else {
      None
    }
  }

  /// Push new value into the end of the vector
  pub fn push(&self, x: T) {
    let mut head = self.head.load(Ordering::Relaxed);
    loop {
      if head < self.capacity {
        let big_idx = head >> Self::SHIFT;
        let small_idx = head & Self::MASK;
        match self.head.compare_exchange_weak(
            head, head+1,
            Ordering::Release, Ordering::Relaxed
        ) {
          Err(new_head) => head = new_head,
          Ok(_) => {
            // At this point we have exclusive access to the slot at head+1. So all we need to do
            // is ensure there are no ongoing writes to conflict with this, and when that's
            // guaranteed, write the value, update and release the new size, and return.
            let mut size = self.size.load(Ordering::Acquire);
            while size != head {
              std::thread::yield_now();
              size = self.size.load(Ordering::Acquire);
            }
            // Now there's no conflict, write + "publish".
            unsafe { *self.blocks[big_idx].data[small_idx].get() = MaybeUninit::new(x) };
            self.size.store(size+1, Ordering::Release);
            return
          }
        }
      } else {
        // Resizing is (should be) *very* inefrequent, so it's actually ok to sync this with a mutex
        unimplemented!()
      }
    }
  }

  pub fn size(&self) -> usize {
    self.size.load(Ordering::Relaxed)
  }

  pub fn pending_writes(&self) -> bool {
    self.size.load(Ordering::Relaxed) != self.head.load(Ordering::Relaxed)
  }
}
