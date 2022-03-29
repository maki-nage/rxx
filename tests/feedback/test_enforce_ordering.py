import time
import functools
import pytest

import rx
import rx.operators as ops
from rx.scheduler import NewThreadScheduler
from rx.subject import Subject
import rxx


@pytest.mark.parametrize("sources,result,completed,lookup_size", [
    pytest.param(
        [
            [1, 1, 6],
            [2],
            [3]
        ], [
            (0, 1), (0, 1), (1, 2), (2, 3), (0, 6),
        ],
        [1, 2, 0],
        1, id="base"
    ),
    pytest.param(
        [
            [1, 1, 6, 2, 3, 5, 4, 7],
            [2, 3, 6, 4, 5, 7],
            [8, 9, 3, 6]
        ], [
            (0, 1), (0, 1), (0, 6), (0, 2), (1, 2),
            (0, 3), (1, 3), (2, 8), (2, 9), (2, 3),
            (0, 5), (0, 4), (1, 6), (1, 4), (1, 5),
            (2, 6), (0, 7),
            (1, 7),
        ],
        [2, 0, 1],
        3, id="lookup3"
    ),
    pytest.param(
        [
            [],
            [],
            []
        ],
        [],
        None,
        3, id="empty sources"
    ),
])
def test_enfore_ordering(sources, result, completed, lookup_size):
    lookup_size = lookup_size
    sources = [rx.from_(s) for s in sources]
    disposables = []
    actual_result = []
    actual_completed = []

    def on_error(e):
        raise e

    def on_next(index, i):
        actual_result.append((index, i))

    def on_completed(index):
        actual_completed.append((index))

    ss = rxx.enforce_ordering(
        sources,
        key_mapper=lambda i: i,
        lookup_size=lookup_size
    )

    for index, source in enumerate(ss):
        disposables.append(source.subscribe(
            on_next=functools.partial(on_next, index),
            on_error=on_error,
            on_completed=functools.partial(on_completed, index),
        ))

    time.sleep(.3)
    assert len(actual_completed) == len(ss)
    if completed is not None:
        assert actual_completed == completed
    assert actual_result == result

    for index, source in enumerate(ss):
        disposables[index].dispose()
