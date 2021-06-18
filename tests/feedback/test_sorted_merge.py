import time
import rx
import rx.operators as ops
from rx.scheduler import NewThreadScheduler
from rx.subject import Subject
import rxx

def test_sorted_merge():
    s1 = Subject()
    s2 = Subject()
    s3 = Subject()

    source = Subject()

    lookup_size = 1
    s1_feedback = []
    s2_feedback = []
    s3_feedback = []
    actual_result=[]
    actual_completed=[]


    def on_back_s1(i): s1_feedback.append(i)
    def on_back_s2(i): s2_feedback.append(i)
    def on_back_s3(i): s3_feedback.append(i)

    def on_error(e):
        raise e

    source.pipe(
        rxx.feedback.sorted_merge(key_mapper=lambda i:i, lookup_size=lookup_size)
    ).subscribe(
        on_next=actual_result.append,
        on_error=on_error,
        on_completed=lambda: actual_completed.append(True),
    )

    on_back = actual_result[0]
    actual_result.clear()

    source.on_next(rxx.Update())
    source.on_next(s1)
    source.on_next(s2)    
    source.on_next(s3)
    source.on_next(rxx.Updated())
    s1.on_next(on_back_s1)
    s2.on_next(on_back_s2)
    s3.on_next(on_back_s3)
    source.on_completed()

    # first cache is filled with feedbacks
    assert s1_feedback == [1]
    assert s2_feedback == [1]
    assert s3_feedback == [1]

    s1.on_next(1) 
    assert actual_result == []

    s2.on_next(2)
    assert actual_result == []

    s3.on_next(3)
    assert actual_result == []

    on_back(1)
    assert actual_result == [1]
    assert s1_feedback == [1, 1]

    s1.on_next(1)
    on_back(1)
    assert actual_result == [1, 1]
    assert s1_feedback == [1, 1, 1]

    s1.on_next(6)
    on_back(1)
    assert actual_result == [1, 1, 2]
    assert s2_feedback == [1, 1]

    s1.on_completed()
    assert actual_result == [1, 1, 2]
    s2.on_completed()
    assert actual_result == [1, 1, 2]
    s3.on_completed()

    on_back(1)
    on_back(1)
    assert actual_result == [1, 1, 2, 3, 6]
    assert actual_completed == [True]


def test_sorted_merge_lookup_3():
    s1 = rx.from_([1, 3, 4, 8, 10, 12, 9, 40]).pipe(
        ops.subscribe_on(NewThreadScheduler()),
        rxx.feedback.pull(),
    )
    s2 = rx.from_([2, 3, 9, 10, 11, 12, 25, 40]).pipe(
        ops.subscribe_on(NewThreadScheduler()),
        rxx.feedback.pull(),
    )
    s3 = rx.from_([3, 15, 18, 10, 22, 26, 40, 42]).pipe(
        ops.subscribe_on(NewThreadScheduler()),
        rxx.feedback.pull(),
    )


    source = rx.from_([rxx.Update(), s1, s2, s3, rxx.Updated()])

    def on_error(e): raise e

    actual_result = []
    actual_completed = []
    source.pipe(
        rxx.feedback.sorted_merge(key_mapper=lambda i:i, lookup_size=3),
        rxx.feedback.push(),
    ).subscribe(
        on_next=actual_result.append,
        on_error=on_error,
        on_completed=lambda: actual_completed.append(True),
    )

    time.sleep(.5)
    assert actual_completed == [True]
    assert actual_result == [1, 2, 3, 3, 3, 4, 8, 10, 12, 9, 9, 10, 15, 18, 10, 11, 12, 22, 25, 26, 40, 40, 40, 42]


def test_sorted_merge_long_sequence():
    s1 = rx.from_(range(1000)).pipe(
        ops.subscribe_on(NewThreadScheduler()),
        rxx.feedback.pull(),
    )
    s2 = rx.from_(range(1000)).pipe(
        ops.subscribe_on(NewThreadScheduler()),
        rxx.feedback.pull(),
    )
    s3 = rx.from_(range(1000)).pipe(
        ops.subscribe_on(NewThreadScheduler()),
        rxx.feedback.pull(),
    )

    source = rx.from_([rxx.Update(), s1, s2, s3, rxx.Updated()])

    def on_error(e): raise e

    actual_result = []
    actual_completed = []
    source.pipe(
        rxx.feedback.sorted_merge(key_mapper=lambda i:i, lookup_size=3),
        rxx.feedback.push(),
    ).subscribe(
        on_next=actual_result.append,
        on_error=on_error,
        on_completed=lambda: actual_completed.append(True),
    )

    time.sleep(.5)
    assert actual_completed == [True]
    assert len(actual_result) == 3000
