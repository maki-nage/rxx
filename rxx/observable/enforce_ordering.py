import threading
from collections import deque
import functools
import rx
import rx.operators as ops
from rx.scheduler import NewThreadScheduler, EventLoopScheduler

import rxx
from rx.disposable import Disposable


class Source(object):
    def __init__(self, source):
        self.buffer = deque()
        self.closest_key = None
        self.on_back = None
        self.source = source
        self.observer = None
        self.disposable = None
        self.is_completed = False
        self.startup = False

    def __repr__(self):
        return "{{closest_key: {}, is_completed: {}, startup: {}, buffer: {}}}".format(
            self.closest_key,
            self.is_completed, self.startup,
            self.buffer,
        )


def enforce_ordering(sources, key_mapper, lookup_size=1):
    """emits items of the sources by respecting the
    incoming order of each element.

    Each items emitted by the source observables are emitted as ordered
    according to a key. Sorting is done in an ascending order. Total ordering
    is not garanteed if the incoming items are not ordered on each source
    observable.

    When ordering on individual sources is not guaranteed, then the lookup_size
    is the depth being used to check for the next items to emit.

    Sources are observables emitting push observables.

    Args:
        key_mapper: A function that maps the sorting key for each item
        lookup_size: [Optional] The buffer size being used on each source to \
            look for the next item to emit.

    Returns:
        Push Observables emitting the same items than the sources observables.
    """
    sources = [
        s.pipe(
            ops.subscribe_on(NewThreadScheduler()),
            rxx.pullable.pull(),
        )
        for s in sources
    ]
    sources = [Source(s) for s in sources]
    startup = True
    all_completed = False
    lock = threading.RLock()
    emit_scheduler = EventLoopScheduler()

    def get_active_source():
        # do not process when some pulls are pending
        # todo use reads state of sorted_merge
        if any([
                len(s.buffer) != lookup_size
                and s.is_completed is False
                for s in sources
        ]):
            return None

        # stop when all sources items have been processed
        if all([
                len(s.buffer) == 0
                and s.is_completed is True
                for s in sources
        ]):
            return None
        return min([s for s in sources if s.closest_key is not None], key=lambda i: i.closest_key)

    def push_next_item(active_source):
        # send all items until we reach the closest key value. If some
        # items are not ordered in this list, there is nothing we can
        # do to fix it. So we emit them as is.
        max_push_count = len(active_source.buffer)
        sent_count = 0
        while sent_count < max_push_count and key_mapper(active_source.buffer[0]) != active_source.closest_key:
            i = active_source.buffer.popleft()
            active_source.observer.on_next(i)
            sent_count += 1

        # send all items matching the key
        while sent_count < max_push_count and key_mapper(active_source.buffer[0]) == active_source.closest_key:
            i = active_source.buffer.popleft()
            active_source.observer.on_next(i)
            sent_count += 1

        # update closest key
        source_len = len(active_source.buffer)
        if source_len > 0:
            keys = [key_mapper(i) for i in active_source.buffer]
            active_source.closest_key = min(keys)
        else:
            active_source.closest_key = None
            if active_source.is_completed:
                active_source.observer.on_completed()

        # request new items
        if active_source.is_completed is False:
            active_source.on_back(lookup_size - source_len)

    def process_reads(scheduler, state):
        if startup is True and all_completed is False:
            return

        with lock:
            while True:
                active_source = get_active_source()
                if active_source is not None:
                    push_next_item(active_source)
                else:
                    break
        return

    def on_subscribe(index, observer, scheduler):
        nonlocal sources
        nonlocal startup
        nonlocal all_completed

        sources[index].observer = observer

        def on_next(source, i):
            with lock:
                nonlocal startup

                # prelude
                if source.on_back is None:
                    source.on_back = i
                    return

                # items
                try:
                    key = key_mapper(i)
                    source.buffer.append(i)
                    if source.closest_key is None or key < source.closest_key:
                        source.closest_key = key

                    if startup is True:
                        if len(source.buffer) == lookup_size:
                            source.startup = False
                        if all([s.startup == False for s in sources]):
                            startup = False
                        else:
                            return

                    emit_scheduler.schedule(process_reads)

                except Exception as e:
                    observer.on_error(e)

        def on_completed(source):
            nonlocal all_completed
            nonlocal startup

            with lock:
                source.is_completed = True

                # clear startup state
                source.startup = False
                if all([s.startup == False for s in sources]):
                    startup = False
                if all([s.is_completed for s in sources]):
                    all_completed = True
                if len(source.buffer) == 0:
                    source.observer.on_completed()
                else:
                    emit_scheduler.schedule(process_reads)

        def fill_all_buffers():
            for _index, s in enumerate(sources):
                if s.is_completed is False:
                    s.on_back(lookup_size - len(s.buffer))

        # wait until all sinks are subscribed before iterating on sources.
        # this prevents out of order initial emission.
        if all([i.observer for i in sources]):
            for source in sources:
                source.disposable = source.source.subscribe(
                    on_next=functools.partial(on_next, source),
                    on_error=observer.on_error,
                    on_completed=functools.partial(on_completed, source),
                    scheduler=scheduler,
                )

            fill_all_buffers()

        def dispose() -> None:
            sources[index].disposable.dispose()

        return Disposable(dispose)

    sinks = [
        rx.create(functools.partial(on_subscribe, index))
        for index, _ in enumerate(sources)
    ]
    return sinks
