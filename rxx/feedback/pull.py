import queue
import rx


def pull(feedback):
    """ Transforms an observable to a pull based observable

    A pull based observable emits items only on request, via a feedback loop.
    The implemetation of the pull is done by blocking the source obeservable on
    the item emission.

    .. marble::
        :alt: pull

        s--0---1---2---3-4----|
        f-1---1---1---2-------|
        [        pull()       ]
        ---0---1---2---3-4----|


    Args:
        feedback: An observable that emits numbers. Each number is a request to emit that count of items.

    Returns:
        An Observable.

    """
    def _pull(source):
        def on_subscribe(observer, scheduler):
            q = queue.Queue()
            remaining = 0

            def on_next_feedback(i):
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

            source_disposable = source.subscribe(
                on_next=on_next,
                on_error=observer.on_error,
                on_completed=observer.on_completed,
                scheduler=scheduler,
            )

            feedback_disposable = feedback.subscribe(
                on_next=on_next_feedback,
                on_error=observer.on_error,
                on_completed=observer.on_completed,
                scheduler=scheduler,
            )

            def dispose():
                source_disposable.dispose()
                feedback_disposable.dispose()

        return rx.create(on_subscribe)

    return _pull
