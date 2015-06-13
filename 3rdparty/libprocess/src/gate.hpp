#ifndef __GATE_HPP__
#define __GATE_HPP__

// TODO(benh): Build implementation directly on-top-of futex's for Linux.

#include <condition_variable>
#include <mutex>

#include <stout/synchronized.hpp>

class Gate
{
public:
  typedef intptr_t state_t;

private:
  int waiters;
  state_t state;
  std::mutex mutex;
  std::condition_variable cond;

public:
  Gate() : waiters(0), state(0) {}

  ~Gate() = default;

  // Signals the state change of the gate to any (at least one) or
  // all (if 'all' is true) of the threads waiting on it.
  void open(bool all = true)
  {
    synchronized (mutex) {
      state++;
      if (all) cond.notify_all();
      else cond.notify_one();
    }
  }

  // Blocks the current thread until the gate's state changes from
  // the current state.
  void wait()
  {
    synchronized (mutex) {
      waiters++;
      state_t old = state;
      while (old == state) {
        std::unique_lock<std::mutex> lock(mutex, std::adopt_lock);
        cond.wait(lock);
        lock.release();
      }
      waiters--;
    }
  }

  // Gets the current state of the gate and notifies the gate about
  // the intention to wait for its state change.
  // Call 'leave()' if no longer interested in the state change.
  state_t approach()
  {
    synchronized (mutex) {
      waiters++;
      return state;
    }
  }

  // Blocks the current thread until the gate's state changes from
  // the specified 'old' state. The 'old' state can be obtained by
  // calling 'approach()'.
  void arrive(state_t old)
  {
    synchronized (mutex) {
      while (old == state) {
        std::unique_lock<std::mutex> lock(mutex, std::adopt_lock);
        cond.wait(lock);
        lock.release();
      }

      waiters--;
    }
  }

  // Notifies the gate that a waiter (the current thread) is no
  // longer waiting for the gate's state change.
  void leave()
  {
    synchronized (mutex) {
      waiters--;
    }
  }

  // Returns true if there is no one waiting on the gate's state
  // change.
  bool empty()
  {
    bool occupied = true;
    synchronized (mutex) {
      occupied = waiters > 0 ? true : false;
    }
    return !occupied;
  }
};

#endif // __GATE_HPP__
