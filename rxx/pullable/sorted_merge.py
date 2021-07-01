from collections import namedtuple, deque
import rx
from rxx.types import NamedObservable, Update, Updated
from rxx.internal.trampoline import Trampoline
from rx.disposable import CompositeDisposable, SingleAssignmentDisposable


class Source(object):
    def __init__(self, on_back):
        self.buffer = deque()
        self.closest_key = None
        self.on_back = on_back
        self.is_completed = False


def sorted_merge(key_mapper, lookup_size=1):
    """Merges a higher order pull based source observable by respecting the
    incoming order of each element.

    Each items emitted by the source observables are emitted as ordered
    according to a key. Sorting is done in an ascending order. Total ordering
    is not garanteed if the incoming items are not ordered on each source
    observable.

    When ordering on individual sources is not guaranteed, then the lookup_size
    is the depth being used to check for the next items to emit.

    Source is a higher-order observable emitting pull observables.

    Args:
        key_mapper: A function that maps the sorting key for each item
        lookup_size: [Optional] The buffer size being used on each source to look for the next item to emit.

    Returns:
        A pull based Observable.
    """
    def _sorted_merge(sources):
        def on_subscribe(observer, scheduler):
            _sources = []
            reads = 0
            sources_completed = False
            all_completed = False
            startup = True
            updating_sources = False # transaction on-going on source update
            pending_source = None  # source with on-going fetch request
            group = CompositeDisposable()
            m = SingleAssignmentDisposable()
            group.add(m)
            trampoline = Trampoline()

            def get_active_source():
                if pending_source is not None:
                    return None

                if len(_sources) == 0:
                    return None

                return min(_sources, key=lambda i: i.closest_key)

            def push_next_item(max_push_count):
                nonlocal pending_source

                if updating_sources is True:
                    return 0

                active_source = get_active_source()
                if active_source is None:
                    return 0

                # send all items until we reach the closest key value. If some
                # items are not ordered in this list, there is nothing we can
                # do to fix it. So we emit them as is.
                sent_count = 0
                while sent_count < max_push_count and key_mapper(active_source.buffer[0]) != active_source.closest_key:
                    i = active_source.buffer.popleft()
                    observer.on_next(i)
                    sent_count += 1

                while sent_count < max_push_count and  key_mapper(active_source.buffer[0]) <= active_source.closest_key:
                    i = active_source.buffer.popleft()
                    observer.on_next(i)
                    sent_count += 1
                    if len(active_source.buffer) == 0:
                        break

                # update closest key
                source_len = len(active_source.buffer)
                if len(active_source.buffer) > 0:
                    keys = [key_mapper(i) for i in active_source.buffer]
                    active_source.closest_key = min(keys)
                else:
                    active_source.closest_key = None
                    if active_source.is_completed is True:
                        _sources.remove(active_source)

                # request new items                
                if active_source.is_completed is False:
                    active_source.on_back(lookup_size - source_len)
                    pending_source = active_source

                return sent_count

            def process_reads():
                nonlocal reads
                if len(_sources) == 0 or startup is True:
                    return

                while reads > 0:
                    send_count = push_next_item(reads)
                    if send_count == 0:
                        break
                    reads -= send_count
                return

            def subscribe_source(s):
                nonlocal pending_source
                nonlocal startup
                source = None
                d = SingleAssignmentDisposable()
                group.add(d)

                def on_next_source(i):
                    with sources.lock:
                        # prelude
                        nonlocal source
                        nonlocal startup
                        nonlocal pending_source
                        if source is None:
                            source = Source(i)
                            source.on_back(lookup_size)
                            _sources.append(source)
                            return

                        # items
                        try:
                            key = key_mapper(i)
                            source.buffer.append(i)
                            if source.closest_key is None or key < source.closest_key:
                                source.closest_key = key

                            if startup is True:
                                if all(len(s.buffer) == lookup_size for s in _sources):
                                    startup = False
                                else:
                                    return

                            #if startup is False:
                            if pending_source is source and len(source.buffer) == lookup_size:
                                pending_source = None
                            trampoline.run(process_reads)

                        except Exception as e:
                            observer.on_error(e)

                def on_completed_source():
                    nonlocal all_completed
                    nonlocal pending_source
                    with sources.lock:
                        group.remove(d)
                        if pending_source is source:
                            pending_source = None
                        if len(source.buffer) == 0:
                            _sources.remove(source)
                        else:
                            source.is_completed = True
                            all_completed = all([i.is_completed for i in _sources])

                        if len(_sources) != 0:
                            trampoline.run(process_reads)

                        if sources_completed is True and all_completed is True and len(_sources) == 0:
                            observer.on_completed()

                d.disposable = s.subscribe(
                    on_next=on_next_source,
                    on_error=observer.on_error,
                    on_completed=on_completed_source
                )

            def on_next(i):
                nonlocal updating_sources
                if type(i) is Update:
                    updating_sources = True
                elif type(i) is Updated:
                    updating_sources = False
                else:
                    subscribe_source(i)

            def on_back(i):
                nonlocal reads
                with sources.lock:
                    reads += i
                    trampoline.run(process_reads)

                if sources_completed and all_completed and len(_sources) == 0:
                    observer.on_completed()

            def on_completed():
                nonlocal sources_completed
                sources_completed = True
                if all_completed is True and len(_sources) == 0:
                    observer.on_completed()

            observer.on_next(on_back)
            m.disposable = sources.subscribe(
                on_next=on_next,
                on_error=observer.on_error,
                on_completed=on_completed,
            )
            return group

        return rx.create(on_subscribe)

    return _sorted_merge
