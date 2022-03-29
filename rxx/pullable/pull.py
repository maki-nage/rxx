import queue
import rx


def pull():
    """ Transforms an observable to a pull based observable

    A pull based observable emits items only on request, via an upstream request path.
    The implementation of the pull is done by blocking the source obeservable on
    the item emission.

    .. marble::
        :alt: pull

        s--0---1---2---3-4----|
        f-1---1---1---2-------|
        [        pull()       ]
        ---0---1---2---3-4----|

    Returns:
        A pull based Observable.

    """
    def _pull(source):
        def on_subscribe(observer, scheduler):
            q = queue.Queue()
            remaining = 0

            def on_back(i):
                if i > 0:
                    q.put(i)

            def on_next(i):
                nonlocal remaining

                if remaining > 0:
                    remaining -= 1
                    observer.on_next(i)
                else:
                    remaining = q.get() - 1
                    observer.on_next(i)

            observer.on_next(on_back)
            return source.subscribe(
                on_next=on_next,
                on_error=observer.on_error,
                on_completed=observer.on_completed,
                scheduler=scheduler,
            )

        return rx.create(on_subscribe)

    return _pull
