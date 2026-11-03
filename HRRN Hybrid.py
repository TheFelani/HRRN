#!/usr/bin/env python3
"""
HRRN (Highest Response Ratio Next) scheduler combining a MONITOR and SEMAPHORES.

Division of labour:
    Monitor HRRNMonitor   owns the ready list and the HRRN policy.
                          Condition variable `nonEmpty` = workers sleep here.
    cpu_slots             counting semaphore, initial value M (number of
                          cores): at most M processes run at the same time.

Golden rule: a thread NEVER waits on a semaphore while inside the monitor.
Lock order is always  cpu_slots -> monitor lock  (never the reverse).

Threads:  W workers (W >= M), 1 dispatcher (handles arrivals).

Run:
    python3 hrrn_hybrid.py
    python3 hrrn_hybrid.py --cores 1 --workers 1
"""
import argparse
import threading
import time

UNIT = 0.02                 # real seconds per simulated millisecond


class Process:
    def __init__(self, name, burst, arrival):
        self.name = name
        self.burst = burst
        self.arrival = arrival      # arrival time in simulated ms
        self.remaining = burst
        self.state = "NEW"
        self.completion = None


# ---------------------------------------------------------------- monitor
class HRRNMonitor:
    def __init__(self, total):
        self._cond = threading.Condition()      # monitor lock + `nonEmpty`
        self._ready_list = []
        self._total = total
        self._finished = 0
        self._closed = False

    def make_ready(self, p):
        with self._cond:
            p.state = "READY"
            self._ready_list.append(p)
            self._cond.notify()                 # signal(nonEmpty)

    def get_next(self, current_time):
        with self._cond:
            while not self._ready_list and not self._closed:   # Mesa: while
                self._cond.wait()
            if not self._ready_list:
                return None                     # everything finished
            
            # HRRN Policy: Select process with the highest Response Ratio
            # Ratio = (Waiting Time + Burst Time) / Burst Time
            best_p = None
            best_ratio = -1.0
            
            for p in self._ready_list:
                waiting_time = max(0.0, current_time - p.arrival)
                ratio = (waiting_time + p.burst) / p.burst
                if ratio > best_ratio:
                    best_ratio = ratio
                    best_p = p
            
            self._ready_list.remove(best_p)
            best_p.state = "RUNNING"
            return best_p

    def mark_terminated(self, p, end):
        with self._cond:
            p.state = "TERMINATED"
            p.completion = end
            self._finished += 1
            if self._finished == self._total:
                self._closed = True
                self._cond.notify_all()


# -------------------------------------------------------------- scheduler
class HybridScheduler:
    def __init__(self, processes, cores, workers):
        self.processes = processes
        self.monitor = HRRNMonitor(len(processes))
        self.cores = cores
        self.workers = max(workers, cores)      # W >= M

        self.cpu_slots = threading.Semaphore(cores)     # counted resource
        self.stop_evt = threading.Event()

        self._free_cores = list(range(1, cores + 1))    # only for logging
        self._core_lock = threading.Lock()
        self._log_lock = threading.Lock()
        self.log = []                                   # (start, end, core, name, arrival)
        self.t0 = 0.0

    def now(self):
        return (time.monotonic() - self.t0) / UNIT

    # --- worker: semaphore -> monitor -> run -> monitor -> semaphore -----
    def _worker(self):
        while True:
            self.cpu_slots.acquire()            # OUTSIDE the monitor: take a core
            current_time = self.now()
            p = self.monitor.get_next(current_time)  # may sleep INSIDE the monitor
            if p is None:                       # everything finished
                self.cpu_slots.release()
                break

            with self._core_lock:
                core = self._free_cores.pop(0)
            
            start = self.now()
            time.sleep(p.burst * UNIT)          # HRRN is non-preemptive: runs full burst
            end = self.now()
            
            with self._core_lock:
                self._free_cores.append(core)

            with self._log_lock:
                self.log.append((start, end, core, p.name, p.arrival))
            
            self.monitor.mark_terminated(p, end)
            self.cpu_slots.release()            # give the core back

    # --- dispatcher: releases processes to monitor as they arrive ----------
    def _dispatcher(self):
        sorted_procs = sorted(self.processes, key=lambda x: x.arrival)
        for p in sorted_procs:
            now_time = self.now()
            delay = p.arrival - now_time
            if delay > 0:
                time.sleep(delay * UNIT)
            if self.stop_evt.is_set():
                break
            self.monitor.make_ready(p)

    def run(self):
        self.t0 = time.monotonic()
        dispatcher = threading.Thread(target=self._dispatcher, daemon=True)
        workers = [threading.Thread(target=self._worker) for _ in range(self.workers)]

        dispatcher.start()
        for w in workers:
            w.start()
        for w in workers:
            w.join()

        self.stop_evt.set()
        dispatcher.join()


def report(s):
    print(f"\nCores: {s.cores}   Workers: {s.workers}")
    print("\nExecution trace")
    print(f"{'Interval':<10}{'Core':<6}{'Process':<9}{'Arrival':<8}")
    for start, end, core, name, arrival in sorted(s.log, key=lambda r: (round(r[0]), r[2])):
        print(f"{round(start):>3}-{round(end):<6}{core:<6}{name:<9}{arrival:<8}")

    print("\nMetrics")
    print(f"{'Process':<9}{'Arrival':>9}{'Completion':>12}{'Turnaround':>12}{'Waiting':>9}")
    tt = wt = 0
    for p in s.processes:
        comp = round(p.completion)
        turn = comp - p.arrival
        wait = turn - p.burst
        tt += turn
        wt += wait
        print(f"{p.name:<9}{p.arrival:>9}{comp:>12}{turn:>12}{wait:>9}")
    n = len(s.processes)
    print(f"{'Average':<9}{'':>9}{'':>12}{tt / n:>12.1f}{wt / n:>9.1f}")


def main():
    ap = argparse.ArgumentParser(description="HRRN with monitor + semaphores")
    ap.add_argument("--cores", type=int, default=2, help="number of cores M (default 2)")
    ap.add_argument--workers = ap.add_argument("--workers", type=int, default=3, help="worker threads W >= M (default 3)")
    args = ap.parse_args()

    # تعریف فرآیندها با نام، مدت زمان پردازش (Burst) و زمان ورود (Arrival)
    processes = [
        Process("A", 10, arrival=0),
        Process("B", 4, arrival=2),
        Process("C", 8, arrival=4)
    ]
    
    sched = HybridScheduler(processes, args.cores, args.workers)
    sched.run()
    report(sched)


if __name__ == "__main__":
    main()
      
