#!/usr/bin/env python3
"""
HRRN (Highest Response Ratio Next) scheduler implemented with a MONITOR.

Python has no `monitor` keyword, so the monitor is a class whose public
methods all run inside `with self._cond:` (implicit mutual exclusion) and
which uses a condition variable for waiting. Python's threading.Condition
has MESA semantics, therefore every wait is inside a `while` loop.
"""
import argparse
import threading
import time
from collections import deque

UNIT = 0.02                 # real seconds per simulated millisecond


class Process:
    def __init__(self, name, burst, arrival=0):
        self.name = name
        self.burst = burst
        self.arrival = arrival
        self.state = "NEW"
        self.completion = None


class HRRNMonitor:
    def __init__(self, total):
        self._cond = threading.Condition()      # monitor lock + condition `nonEmpty`
        self._ready_queue = deque()
        self._ready_count = 0
        self._total = total
        self._finished = 0
        self._closed = False

    # --- internal (caller must already be inside the monitor) ------------
    def _enqueue(self, p):
        self._ready_queue.append(p)
        p.state = "READY"
        self._ready_count += 1
        self._cond.notify()                     # signal(nonEmpty)

    # --- entry procedures -------------------------------------------------
    def make_ready(self, p):
        with self._cond:
            self._enqueue(p)

    def get_next(self, current_time):
        """Block until a process exists; return the one with the highest response ratio.
        Ratio = (Waiting Time + Burst Time) / Burst Time
        Returns None once every process has terminated."""
        with self._cond:
            while self._ready_count == 0 and not self._closed:   # Mesa: use while
                self._cond.wait()                                # releases the lock
            if self._ready_count == 0:
                return None
            
            # Find process with the highest response ratio
            best_p = None
            best_ratio = -1.0
            
            for p in self._ready_queue:
                waiting_time = current_time - p.arrival
                # HRRN ratio formula: (W + S) / S
                ratio = (waiting_time + p.burst) / p.burst
                if ratio > best_ratio:
                    best_ratio = ratio
                    best_p = p
            
            # Remove selected process from the queue
            self._ready_queue.remove(best_p)
            self._ready_count -= 1
            best_p.state = "RUNNING"
            return best_p

    def update_after_run(self, p, end):
        with self._cond:
            p.state = "TERMINATED"
            p.completion = end
            self._finished += 1
            if self._finished == self._total:
                self._closed = True
                self._cond.notify_all()         # wake the dispatcher so it can exit


class Scheduler:
    def __init__(self, processes):
        self.processes = processes
        self.monitor = HRRNMonitor(len(processes))
        self.stop_evt = threading.Event()
        self.log = []
        self.t0 = 0.0

    def now(self):
        return (time.monotonic() - self.t0) / UNIT

    def _dispatcher(self):
        while True:
            current_time = self.now()
            p = self.monitor.get_next(current_time)     # may sleep inside the monitor
            if p is None:
                break
            
            start = self.now()
            time.sleep(p.burst * UNIT)                  # run OUTSIDE the monitor (non-preemptive)
            end = self.now()
            
            self.log.append((start, end, p.name))
            self.monitor.update_after_run(p, end)

    def run(self):
        self.t0 = time.monotonic()
        for p in self.processes:
            self.monitor.make_ready(p)
        self._dispatcher()


def report(s):
    print("\nExecution trace")
    print(f"{'Interval':<10}{'Process':<9}{'Scheduler Queue':<16}")
    for start, end, name in s.log:
        print(f"{round(start):>3}-{round(end):<6}{name:<9}Ready")

    print("\nMetrics")
    print("Algorithm: HRRN (Highest Response Ratio Next)")
    print(f"{'Process':<9}{'Completion':>11}{'Turnaround':>12}{'Waiting':>9}")
    tt = wt = 0
    for p in s.processes:
        comp = round(p.completion)
        turn = comp - p.arrival
        wait = turn - p.burst
        tt += turn
        wt += wait
        print(f"{p.name:<9}{comp:>11}{turn:>12}{wait:>9}")
    n = len(s.processes)
    print(f"{'Average':<9}{'':>11}{tt / n:>12.1f}{wt / n:>9.1f}")
    print(f"\nPriority boosts performed: 0")


def main():
    ap = argparse.ArgumentParser(description="HRRN with a monitor")
    args = ap.parse_args()

    # نمونه فرآیندها با زمان ورود و برست متناسب با نمونه استاندارد HRRN
    processes = [
        Process("A", 8, arrival=0),
        Process("B", 6, arrival=2),
        Process("C", 12, arrival=4),
        Process("D", 4, arrival=6),
        Process("E", 8, arrival=8)
    ]
    
    sched = Scheduler(processes)
    sched.run()
    report(sched)


if __name__ == "__main__":
    main()
      
