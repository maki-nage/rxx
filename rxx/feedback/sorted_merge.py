from collections import namedtuple, deque
import rx
from rxx.types import NamedObservable, Update, Updated
from rx.disposable import CompositeDisposable, SingleAssignmentDisposable


class Source(object):
    def __init__(self, on_back):
        self.buffer = deque()
        self.closest_key = None
        self.on_back = on_back
        self.is_completed = False


def sorted_merge(key_mapper, lookup_size):
    """

    Source is a higher-order obbservable emitting NamedObservable items.
    """
    def _sorted_merge(sources):
        def on_subscribe(observer, scheduler):
            _sources = []
            sources_completed = False
            updating_sources = False
            group = CompositeDisposable()
            m = SingleAssignmentDisposable()
            group.add(m)

            def drain():
                all_completed = all([i.is_completed for i in _sources])
                if not all_completed:
                    return False

                while len(_sources) > 0:
                    push_next_item(drain=True)
                return True

            def push_next_item(drain=False):
                nonlocal updating_sources
                if updating_sources is True:
                    return

                active_source = None
                for source in _sources:
                    if drain is False:
                        if len(source.buffer) != lookup_size and not source.is_completed:
                            return

                    if active_source is None or source.closest_key < active_source.closest_key:
                        active_source = source

                # send all items until we reach the closest key value. If some
                # items are not ordered in this list, there is nothing we can
                # do to fix it. So we emit them as is.
                while key_mapper(active_source.buffer[0]) != active_source.closest_key:
                    i = active_source.buffer.popleft()
                    observer.on_next(i)

                while key_mapper(active_source.buffer[0]) <= active_source.closest_key:
                    i = active_source.buffer.popleft()
                    observer.on_next(i)
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

            def subscribe_source(s):
                source = None
                d = SingleAssignmentDisposable()
                group.add(d)

                def on_next_source(i):
                    with sources.lock:
                        # prelude
                        nonlocal source
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

                            push_next_item()

                        except Exception as e:
                            observer.on_error(e)

                def on_completed_source():
                    with sources.lock:
                        group.remove(d)
                        if len(source.buffer) == 0:
                            _sources.remove(source)
                        else:
                            source.is_completed = True
                            push_next_item()

                        if sources_completed is True and drain() is True:
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

            def on_completed():
                nonlocal sources_completed
                sources_completed = True
                if drain() is True:
                    observer.on_completed()

            m.disposable = sources.subscribe(
                on_next=on_next,
                on_error=observer.on_error,
                on_completed=on_completed,
            )
            return group

        return rx.create(on_subscribe)

    return _sorted_merge
