import rxx
from rx.subject import Subject


def test_push():
    source = Subject()

    actual_result = []
    actual_back = []
    actual_completed = []

    def on_back(i): actual_back.append(i)
    def on_error(e): raise e    

    source.pipe(rxx.feedback.push()).subscribe(
        on_next=actual_result.append,
        on_error=on_error,
        on_completed=lambda: actual_completed.append(True),
    )

    source.on_next(on_back)
    assert actual_back == [1]
    source.on_next(1)
    assert actual_result == [1]

    assert actual_back == [1, 1]
    source.on_next(2)
    assert actual_result == [1, 2]

    source.on_completed()
    assert actual_completed == [True]