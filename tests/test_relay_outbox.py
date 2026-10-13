"""Core level: the Annex D checks of the relay with fakes."""

from tests.fakes import Clock, ListOutbox, Publisher, event
from worker.application.port.inbound.job import RunContext
from worker.application.usecase.relay_outbox import RelayOutbox
from worker.domain.event import OutboxEvent


def context(deadline=1_000.0):
    return RunContext("run-corr-1", deadline)


def test_three_events_with_a_failure_in_the_second_publish_two_count_one_and_continue():
    events = [event(n=1), event(n=2), event(n=3)]
    outbox, publisher = ListOutbox("catalog", events), Publisher({events[1].id})
    report = RelayOutbox([outbox], publisher, Clock(), batch_size=100).run(context())
    assert (report.processed, report.failed, report.timed_out) == (2, 1, False)
    assert [e.id for e in outbox.published] == [events[0].id, events[2].id] and outbox.pending == [events[1]]


def test_a_run_asks_exactly_the_batch_size_and_carries_its_correlation():
    outbox, publisher = ListOutbox("billing", [event("billing", "TuitionPaid", n) for n in range(5)]), Publisher()
    report = RelayOutbox([outbox], publisher, Clock(), batch_size=3).run(context())
    assert outbox.asked == [3] and report.processed == 3
    assert {c for _, c in publisher.sent} == {"run-corr-1"}


def test_a_run_past_its_deadline_takes_nothing_more():
    outbox = ListOutbox("enrollment", [event("enrollment", "EnrollmentConfirmed", n) for n in range(4)])
    report = RelayOutbox([outbox], Publisher(), Clock(), batch_size=100).run(context(deadline=3.0))
    assert report.timed_out and report.processed < 4 and outbox.pending


def test_an_unreachable_outbox_does_not_stop_the_others():
    broken, healthy = ListOutbox("billing", [], broken=True), ListOutbox("catalog", [event(n=1)])
    report = RelayOutbox([broken, healthy], Publisher(), Clock(), batch_size=10).run(context())
    assert (report.processed, report.failed) == (1, 1)


def test_a_second_run_publishes_only_what_is_left():
    events = [event(n=1), event(n=2)]
    outbox, publisher = ListOutbox("catalog", events), Publisher({events[0].id})
    relay = RelayOutbox([outbox], publisher, Clock(), batch_size=10)
    relay.run(context())
    publisher.failing.clear()
    assert relay.run(context()).processed == 1 and [e.id for e, _ in publisher.sent] == [events[1].id, events[0].id]


def test_the_routing_key_and_the_message():
    e = event("catalog", "SeatReserved", 7)
    assert e.routing_key == "catalog.SeatReserved"
    message = e.message()
    assert message["eventId"] == str(e.id) and message["payload"] == {"n": 7} and message["occurredAt"].endswith("Z")
    assert isinstance(e, OutboxEvent)
