import time
import rx
import rx.operators as ops
from rx.subject import Subject
from rx.scheduler import NewThreadScheduler

import rxx


def test_pull():
    source = [1, 2, 3, 4, 5, 6, 7, 8]

    actual_result = []
    actual_error = []
    actual_completed = []

    rx.from_(source).pipe(
        ops.subscribe_on(NewThreadScheduler()),
        rxx.pullable.pull()
    ).subscribe(
        on_next=actual_result.append,
        on_error=actual_error.append,
        on_completed=lambda: actual_completed.append(True),
    )

    assert len(actual_result) == 1
    on_back = actual_result[0]
    actual_result.clear()

    on_back(1)
    time.sleep(0.1)
    assert actual_result == [1]
    actual_result.clear()

    on_back(-1)
    time.sleep(0.1)
    assert actual_result == []

    on_back(3)
    time.sleep(0.1)
    assert actual_result == [2, 3, 4]
    actual_result.clear()

    on_back(6)
    time.sleep(0.1)
    assert actual_result == [5, 6, 7, 8]
    actual_result.clear()

    assert actual_completed == [True]
