from threading import Event, Thread
from collections import Iterable
import random


class RepeatingTimer(Thread):
    def __init__(self, term,  interval, function, args=None, kwargs=None):
        self.term = term
        Thread.__init__(self)
        if not isinstance(interval, Iterable):
            interval = [interval] * 2
        self.interval = interval[:2]
        self.function = function
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}
        self.finished = Event()
        self.start()

    def cancel(self):
        self.finished.set()
        # self.join()

    def run(self):
        while not self.finished.is_set():
            self.function(*self.args, **self.kwargs)
            if any([i > 0 for i in self.interval]):
                self.finished.wait(random.uniform(*self.interval))


class OnceTimer(Thread):

    def __init__(self, term, interval, function, args=None, kwargs=None):
        Thread.__init__(self)
        self.term = term
        if not isinstance(interval, Iterable):
            interval = [interval] * 2
        self.interval = random.uniform(*interval[:2])
        self.function = function
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}
        self.finished = Event()
        self.start()

    def cancel(self):
        self.finished.set()
        # self.join()

    def run(self):
        self.finished.wait(self.interval)
        if not self.finished.is_set():
            self.function(*self.args, **self.kwargs)
        self.finished.set()
