import rx


def push():
    """ Transforms a pull based observable to an Observable

    Returns:
        An Observable.

    """
    def _push(source):
        def on_subscribe(observer, scheduler):
            on_back = None

            def on_next(i):
                nonlocal on_back

                if on_back is None:
                    on_back = i
                    on_back(1)
                    return

                observer.on_next(i)
                on_back(1)

            return source.subscribe(
                on_next=on_next,
                on_error=observer.on_error,
                on_completed=observer.on_completed,
                scheduler=scheduler,
            )

        return rx.create(on_subscribe)
    return _push
