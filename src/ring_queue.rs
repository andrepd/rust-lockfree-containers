use std::{sync::atomic::{AtomicUsize, Ordering}, mem::MaybeUninit, cell::UnsafeCell};

// TODO: minimise false sharing, by making it so that subsequent elements don't go in adjacent
// cache lines (this is a case where memory locality doesn't necessariliy mean better
// performance!)

/// A lock-free ring queue of a fixed size. It is lock-free but not *wait*-free, as `push`ing
/// blocks while the queue is full (until some `pop` drains a space), and `pop`pping blocks while
/// the queue is empty (until some `push` gives us a value to return).
#[derive(Debug)]
pub struct Queue<T, const N_ORIG: usize = 4096> 
  where [UnsafeCell<MaybeUninit<T>>; next_pow2(N_ORIG)]: Sized
{
  // Sounds scary and unsafe, but note we actually don't ever return references to elements of
  // `data`, we just move from and into it, therefore the correctness invariants that we have to
  // keep in mind are much simplified; we basically just need to ensure we don't overwrite
  // elements that haven't been moved out, and don't move out cells that haven't been written to.
  data: [UnsafeCell<MaybeUninit<T>>; next_pow2(N_ORIG)],
  // TODO: look into how to add padding between these elements, to avoid false sharing
  write_head: AtomicUsize,
  write_tail: AtomicUsize,
  read_head: AtomicUsize,
  read_tail: AtomicUsize,
}

unsafe impl<T, const N_ORIG: usize> Send for Queue<T, N_ORIG> 
  where [UnsafeCell<MaybeUninit<T>>; next_pow2(N_ORIG)]: Sized
  {}
unsafe impl<T, const N_ORIG: usize> Sync for Queue<T, N_ORIG> 
  where [UnsafeCell<MaybeUninit<T>>; next_pow2(N_ORIG)]: Sized
  {}

const fn next_pow2(x: usize) -> usize {
  // https://graphics.stanford.edu/%7Eseander/bithacks.html#RoundUpPowerOf2
  let x = x - 1;
  let x = x | (x >> 1);
  let x = x | (x >> 2);
  let x = x | (x >> 4);
  let x = x | (x >> 8);
  let x = x | (x >> 16);
  let x = x | (x >> 32);
  x + 1
}

// Buffer layout diagram (wraps modulo N):
//
// INVALID  |  READING  |       OKAY       |  WRITING   |           INVALID
//          ^           ^                  ^            ^
//          read_head   read_tail          write_head   write_tail

impl<T, const N_ORIG: usize> Queue<T, N_ORIG>
  where [UnsafeCell<MaybeUninit<T>>; next_pow2(N_ORIG)]: Sized
{
  const N: usize = next_pow2(N_ORIG);

  const EMPTY: UnsafeCell<MaybeUninit<T>> = UnsafeCell::new(MaybeUninit::uninit());

  const MASK: usize = Self::N - 1;

  pub fn new() -> Self {
    Self {
      data: [Self::EMPTY; next_pow2(N_ORIG)],
      write_head: AtomicUsize::new(0),
      read_head: AtomicUsize::new(0),
      write_tail: AtomicUsize::new(0),
      read_tail: AtomicUsize::new(0),
    }
  }

  /// Returns the number of elements in the queue.
  pub fn size(&self) -> usize {
    // Ok to load with Relaxed: there is always a race here
    self.write_head.load(Ordering::Relaxed) - self.read_tail.load(Ordering::Relaxed)
  }

  /// Push a value onto the queue, blocking if the queue is full.
  pub fn push(&self, x: T) {
    // Acquire exclusive "ownership" of the element at `write_head`, by using compare-and-swap to
    // ensure there is no data race, and no two threads attempt to write to the same slot
    let mut write_head = self.write_head.load(Ordering::Relaxed);
    loop {
      // Remember to use order `Acquire`, to ensure we get the value of `read_tail` that other
      // threads pushed in their `pop` with order `Release`.
      let read_tail = self.read_tail.load(Ordering::Acquire);
      // If write_head == read_tail + N, the queue is full (we would be overwriting existing
      // elements if we wrote to write_head)
      if write_head < read_tail + Self::N {
        // Now this is where we use CAS to ensure that we are incrementing write_head. If the value
        // has changed in the meantime, `compare_exchange_weak` will fail, let us now the updated
        // value of write_head, and we try again with that value
        match self.write_head.compare_exchange_weak(
            write_head, write_head+1,
            Ordering::Release, Ordering::Relaxed
        ) {
          Err(new_head) => write_head = new_head,
          Ok(_) => break,
        }
      }
    }
    // At this point we have exclusive ownership of this slot. Let's write (move) `x` to that slot,
    // then "publish" the result by incrementing the `write_tail` pointer. Before we do, we must
    // ensure no other thread write is "in-flight", or when we increment write_tail we might be
    // publishing another thread's (possibly still invalid) write! As this copy is fast, this
    // should not a significant source of slowdown.
    let mut write_tail = self.write_tail.load(Ordering::Acquire);
    while write_tail != write_head {
      std::thread::yield_now();
      write_tail = self.write_tail.load(Ordering::Acquire);
    }
    // Now, finally copy the value and "publish" by incrementing `write_tail`, with `Release`
    // order.
    unsafe { *self.data[write_tail & Self::MASK].get() = MaybeUninit::new(x) };
    self.write_tail.store(write_tail+1, Ordering::Release);
  }

  /// Pop a value from the queue, blocking if the queue is empty.
  pub fn pop(&self) -> T {
    // Same basic skeleton as `push`: acquire exclusive "ownership" of the element at `read_head`,
    // by using compare-and-swap to ensure there is no data race, and no two threads attempt to
    // read from the same slot. Refer to the comments in `push`.
    let mut read_head = self.read_head.load(Ordering::Relaxed);
    loop {
      // Now, we are `Acquire`ing the changes that `push` has `Release`d.
      let write_tail = self.write_tail.load(Ordering::Acquire);
      if read_head < write_tail {
        match self.read_head.compare_exchange_weak(
            read_head, read_head+1,
            Ordering::Release, Ordering::Relaxed
        ) {
          Err(new_head) => read_head = new_head,
          Ok(_) => break,
        }
      }
    }
    let mut read_tail = self.read_tail.load(Ordering::Acquire);
    while read_tail != read_head {
      std::thread::yield_now();
      read_tail = self.read_tail.load(Ordering::Acquire);
    }
    // Now, finally copy out the value (first) and "publish" the change by incrementing
    // `read_tail` (after).
    let x = unsafe { self.data[read_tail & Self::MASK].get().read().assume_init() };
    self.read_tail.store(read_tail+1, Ordering::Release);
    x
  }

  pub fn is_empty(&self) -> bool {
    self.write_head.load(Ordering::Relaxed) == self.read_tail.load(Ordering::Relaxed)
  }

  pub fn is_full(&self) -> bool {
    self.write_head.load(Ordering::Relaxed) == self.read_tail.load(Ordering::Relaxed) + Self::N
  }
}

#[cfg(test)]
mod test {
  use super::*;

  // Crude tests. TODO: use `loom`.
  #[test]
  fn stress() {
    // Change these to try different numbers of producers and consumers
    const N_READERS: usize = 2;
    const N_WRITERS: usize = 2;

    // Change these to introduce delay to one or both of them
    const READER_DELAY: std::time::Duration = std::time::Duration::from_millis(0);
    const WRITER_DELAY: std::time::Duration = std::time::Duration::from_millis(0);

    let queue = Queue::<(usize, usize)>::new();

    std::thread::scope(|s| {
      for _ in 0..N_READERS {
        let queue = &queue;
        s.spawn(move || {
          /*println!("Spawn reader {thread_id}");*/
          let mut highest = [-1isize; N_WRITERS];
          loop {
            let (thread_id, counter) = queue.pop();
            if N_READERS == 1 {
              assert!(counter as isize == highest[thread_id] + 1)
            } else {
              assert!(counter as isize > highest[thread_id]);
            }
            highest[thread_id] = counter as isize;
            /*println!("r {thread_id} {counter}");*/
            std::thread::sleep(READER_DELAY);
          }
        });
      };

      for thread_id in 0..N_WRITERS {
        let queue = &queue;
        s.spawn(move || {
          /*println!("Spawn writer {thread_id}");*/
          let mut counter = 0usize;
          loop {
            queue.push((thread_id, counter));
            counter += 1;
            /*println!("w {thread_id} {counter}");*/
            std::thread::sleep(WRITER_DELAY);
          }
        });
      };
    });
  }
}
