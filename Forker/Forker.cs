/* Forker 1.0 - (C) 2016 Premysl Fara 
 
Forker 1.0 and newer are available under the zlib license :

This software is provided 'as-is', without any express or implied
warranty.  In no event will the authors be held liable for any damages
arising from the use of this software.

Permission is granted to anyone to use this software for any purpose,
including commercial applications, and to alter it and redistribute it
freely, subject to the following restrictions:

1. The origin of this software must not be misrepresented; you must not
   claim that you wrote the original software. If you use this software
   in a product, an acknowledgment in the product documentation would be
   appreciated but is not required.
2. Altered source versions must be plainly marked as such, and must not be
   misrepresented as being the original software.
3. This notice may not be removed or altered from any source distribution.
 
 */

namespace Forker
{
    using System;
    using System.Collections.Generic;
    using System.Threading;


    /// <summary>
    /// Event arguments representing the completion of a parallel action.
    /// </summary>
    public class ParallelEventArgs : EventArgs
    {
        private readonly object _state;
        private readonly Exception _exception;


        internal ParallelEventArgs(object state, Exception exception)
        {
            _state = state;
            _exception = exception;
        }

        /// <summary>
        /// The opaque state object that identifies the action (null otherwise).
        /// </summary>
        public object State
        {
            get { return _state; }
        }

        /// <summary>
        /// The exception thrown by the parallel action, or null if it completed without exception.
        /// </summary>
        public Exception Exception
        {
            get { return _exception; }
        }
    }


    /// <summary>
    /// Provides a caller-friendly wrapper around parallel actions.
    /// </summary>
    public sealed class Forker
    {
        public Forker(int maxAllowed)
        {
            _maxAllowed = (maxAllowed <= 0) ? Int32.MaxValue : maxAllowed;
        }


        private readonly object _joinLock = new object();
        private readonly object _eventLock = new object();

        private int _running;
        private readonly int _maxAllowed;
        private readonly Queue<ThreadStart> _actionQueue = new Queue<ThreadStart>();


        /// <summary>
        /// Raised when all operations have completed.
        /// </summary>
        public event EventHandler AllComplete
        {
            add
            {
                lock (_eventLock) { _allComplete += value; }
            }

            remove
            {
                lock (_eventLock) { _allComplete -= value; }
            }
        }

        private EventHandler _allComplete;


        /// <summary>
        /// Raised when each operation completes.
        /// </summary>
        public event EventHandler<ParallelEventArgs> ItemComplete
        {
            add
            {
                lock (_eventLock) { _itemComplete += value; }
            }

            remove
            {
                lock (_eventLock) { _itemComplete -= value; }
            }
        }

        private EventHandler<ParallelEventArgs> _itemComplete;


        private void OnItemComplete(object state, Exception exception)
        {
            var itemHandler = _itemComplete; // don't need to lock
            if (itemHandler != null)
            {
                itemHandler(this, new ParallelEventArgs(state, exception));
            }

            // Lower the count of running tasks.
            var running = Interlocked.Decrement(ref _running);

            // If we have something queued...
            if (_actionQueue.Count > 0)
            {
                // ... run it.
                StartAction(_actionQueue.Dequeue(), state);

#if DEBUG

                Console.WriteLine("*** QUEUED TASK EXECUTED ***");

#endif

                return;
            }

            // If the last running task just finished...
            if (running == 0)
            {
                var allHandler = _allComplete; // don't need to lock
                if (allHandler != null)
                {
                    allHandler(this, EventArgs.Empty);
                }

                lock (_joinLock)
                {
                    Monitor.PulseAll(_joinLock);
                }
            }
        }

        /// <summary>
        /// Adds a callback to invoke when each operation completes.
        /// </summary>
        /// <returns>Current instance (for fluent API).</returns>
        public Forker OnItemComplete(EventHandler<ParallelEventArgs> handler)
        {
            if (handler == null) throw new ArgumentNullException("handler");

            ItemComplete += handler;

            return this;
        }

        /// <summary>
        /// Adds a callback to invoke when all operations are complete.
        /// </summary>
        /// <returns>Current instance (for fluent API).</returns>
        public Forker OnAllComplete(EventHandler handler)
        {
            if (handler == null) throw new ArgumentNullException("handler");

            AllComplete += handler;

            return this;
        }

        /// <summary>
        /// Waits for all operations to complete.
        /// </summary>
        public void Join()
        {
            Join(-1);
        }

        /// <summary>
        /// Waits (with timeout) for all operations to complete.
        /// </summary>
        /// <returns>Whether all operations had completed before the timeout.</returns>
        public bool Join(int millisecondsTimeout)
        {
            lock (_joinLock)
            {
                if (CountRunning() == 0) return true;

                Thread.SpinWait(1); // try our luck...

                return (CountRunning() == 0) || Monitor.Wait(_joinLock, millisecondsTimeout);
            }
        }

        /// <summary>
        /// Indicates the number of incomplete operations.
        /// </summary>
        /// <returns>The number of incomplete operations.</returns>
        public int CountRunning()
        {
            return Interlocked.CompareExchange(ref _running, 0, 0);
        }

        /// <summary>Enqueues an operation.</summary>
        /// <param name="action">The operation to perform.</param>
        /// <returns>The current instance (for fluent API).</returns>
        public Forker Fork(ThreadStart action)
        {
            return Fork(action, null);
        }

        /// <summary>
        /// Enqueues an operation.
        /// </summary>
        /// <param name="action">The operation to perform.</param>
        /// <param name="state">An opaque object, allowing the caller to identify operations.</param>
        /// <returns>The current instance (for fluent API).</returns>
        public Forker Fork(ThreadStart action, object state)
        {
            if (action == null) throw new ArgumentNullException("action");

            // Do not run infinite amount od tasks...
            if (CountRunning() >= _maxAllowed)
            {
                _actionQueue.Enqueue(action);

#if DEBUG

                Console.WriteLine("*** TASK QUEUED ***");

#endif

                return this;
            }

            StartAction(action, state);

#if DEBUG

            Console.WriteLine("*** TASK EXECUTED ***");

#endif

            return this;
        }


        private void StartAction(ThreadStart action, object state)
        {
            Interlocked.Increment(ref _running);

            ThreadPool.QueueUserWorkItem(delegate
            {
                Exception exception = null;
                try
                {
                    action();
                }
                catch (Exception ex)
                {
                    exception = ex;
                }

                OnItemComplete(state, exception);
            });
        }
    }
}
