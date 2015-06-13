#ifndef __PROCESS_ONCE_HPP__
#define __PROCESS_ONCE_HPP__

#include <condition_variable>
#include <mutex>

#include <process/future.hpp>

#include <stout/nothing.hpp>
#include <stout/synchronized.hpp>

namespace process {

// Provides a _blocking_ abstraction that's useful for performing a
// task exactly once.
class Once
{
public:
  Once() : started(false), finished(false) {}

  ~Once() = default;

  // Returns true if this Once instance has already transitioned to a
  // 'done' state (i.e., the action you wanted to perform "once" has
  // been completed). Note that this BLOCKS until Once::done has been
  // called.
  bool once()
  {
    bool result = false;

    synchronized (mutex) {
      if (started) {
        while (!finished) {
          std::unique_lock<std::mutex> lock(mutex, std::adopt_lock);
          cond.wait(lock);
          lock.release();
        }
        result = true;
      } else {
        started = true;
      }
    }

    return result;
  }

  // Transitions this Once instance to a 'done' state.
  void done()
  {
    synchronized (mutex) {
      if (started && !finished) {
        finished = true;
        cond.notify_all();
      }
    }
  }

private:
  // Not copyable, not assignable.
  Once(const Once& that);
  Once& operator = (const Once& that);

  std::mutex mutex;
  std::condition_variable cond;
  bool started;
  bool finished;
};

}  // namespace process {

#endif // __PROCESS_ONCE_HPP__
