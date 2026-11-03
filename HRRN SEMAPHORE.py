#!/usr/bin/env python3
"""
HRRN (Highest Response Ratio Next) scheduler implemented with SEMAPHORES ONLY.

Design matches your semaphore-based style:
    mutex       binary semaphore (1)  protects the ready queue
    ready       counting semaphore (0) number of ready processes; dispatcher sleeps on it
    cpu         binary semaphore (1)  only one process/thread owns the CPU
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
        self.remaining = burst
        self.state = "NEW"
        self.completion = None
        self.waiting_time = 0
        self.turnaround_time = 0


class SemaphoreHRRN:
    def __init__(self):
        self.queue = deque()
        self.mutex = threading.Semaphore(1)
        self.ready = threading.Semaphore(0)
        self.cpu = threading.Semaphore(1)

        self.total = 0
        self.finished = 0
        self.log = []               # (start, end, process name)
        self.t0 = 0.0

    # ------------------------------------------------------------ helpers
    def now(self):
        return (time.monotonic() - self.t0) / UNIT

    # -------------------------------------------------------- make_ready
    def make_ready(self, p):
        self.mutex.acquire()
        self.queue.append(p)
        p.state = "READY"
        self.mutex.release()
        self.ready.release()        # wake the dispatcher

    # -------------------------------------------------------- dispatcher
    def _pick_next(self, current_time):
        """
        Dequeue the process with the highest Response Ratio:
        Response Ratio = (Waiting Time + Burst Time) / Burst Time
        """
        self.mutex.acquire()
        chosen = None
        if self.queue:
            best_ratio = -1.0
            best_index = -1
            
            for idx, p in enumerate(self.queue):
                waiting_time = current_time - p.arrival
                # فرمول نسبت پاسخ (HRRN)
                ratio = (waiting_time + p.burst) / p.burst
                if ratio > best_ratio:
                    best_ratio = ratio
                    best_index = idx
            
            if best_index != -1:
                # حذف فرآیند انتخاب شده از صف
                chosen = self.queue[best_index]
                del self.queue[best_index]
                
        self.mutex.release()
        return chosen

    def dispatcher(self):
        while self.finished < self.total:
            self.ready.acquire()                    # sleep if nothing is ready
            
            current_time = self.now()
            p = self._pick_next(current_time)
            
            if p is None:                           # defensive
                continue
                
            p.state = "RUNNING"
            
            self.cpu.acquire()                      # own the CPU
            start = self.now()
            
            # HRRN غیرپری‌پتیو است؛ بنابراین فرآیند تا پایان کامل burst اجرا می‌شود
            time.sleep(p.burst * UNIT)             
            end = self.now()
            
            self.cpu.release()

            p.completion = end
            p.turnaround_time = p.completion - p.arrival
            p.waiting_time = p.turnaround_time - p.burst
            p.state = "TERMINATED"
            
            self.log.append((start, end, p.name))
            self.finished += 1

    # --------------------------------------------------------------- run
    def run(self, processes):
        self.total = len(processes)
        self.t0 = time.monotonic()
        
        # مدیریت زمان ورود فرآیندها (Arrival Time) در صورت داشتن تأخیر
        # برای سادگی فرض می‌کنیم فرآیندها به محض رسیدن به صف اضافه می‌شوند، 
        # یا می‌توانیم برای هر کدام یک ترد ساخت که بعد از زمان arrival به صف اضافه کند.
        
        threads = []
        for p in processes:
            def delayed_ready(proc):
                if proc.arrival > 0:
                    time.sleep(proc.arrival * UNIT)
                self.make_ready(proc)
            
            t = threading.Thread(target=delayed_ready, args=(p,))
            threads.append(t)
            t.start()

        # شروع به کار دیسپچر
        disp_thread = threading.Thread(target=self.dispatcher)
        disp_thread.start()

        for t in threads:
            t.join()
        disp_thread.join()


def report(sched, processes):
    print("\nExecution trace (HRRN)")
    print(f"{'Interval':<10}{'Process':<9}")
    for start, end, name in sched.log:
        print(f"{round(start):>3}-{round(end):<6}{name:<9}")

    print("\nMetrics")
    print(f"{'Process':<9}{'Completion':>11}{'Turnaround':>12}{'Waiting':>9}")
    tt = wt = 0
    for p in processes:
        comp = round(p.completion)
        turn = round(p.turnaround_time)
        wait = round(p.waiting_time)
        tt += turn
        wt += wait
        print(f"{p.name:<9}{comp:>11}{turn:>12}{wait:>9}")
        
    n = len(processes)
    print(f"{'Average':<9}{'':>11}{tt / n:>12.1f}{wt / n:>12.1f}")


def main():
    ap = argparse.ArgumentParser(description="HRRN with semaphores")
    args = ap.parse_args()

    # تعریف فرآیندها با نام، زمان برست (Burst) و زمان ورود (Arrival Time)
    processes = [
        Process("A", 20, arrival=0), 
        Process("B", 6, arrival=2), 
        Process("C", 12, arrival=4)
    ]
    
    sched = SemaphoreHRRN()
    sched.run(processes)
    report(sched, processes)


if __name__ == "__main__":
    main()
