"""B integration checks against C's real repository, using disposable database copies."""
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'A2/src'))
from database.repository import EventRepository
from subscription.event_processor import EventProcessor, InvalidMessage
from subscription.state_store import StateStore
from subscription.run import SubscriptionService


def topic(event):
    return f"comp5339/ev/{event['station_id']}/{event['event_type']}"


class MemberBTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'test.duckdb'
        shutil.copy2(ROOT / 'A2/data/final/ev_chargers_a2.duckdb', self.path)
        self.repo = EventRepository(self.path)
        # The tracked database is an initialised empty event store.
        self.assertEqual(self.repo.get_historical_metrics()['completed_session_count'], 0)
        self.state = StateStore()
        self.processor = EventProcessor('comp5339/ev', self.state, self.repo)
        self.events = [json.loads(s) for s in (ROOT / 'A2/data/demo/event_stream.jsonl').read_text().splitlines()]
        self.sid = next(e['payload']['session_id'] for e in self.events if e['event_type'] == 'session_start')
        self.station = self.events[0]['station_id']

    def tearDown(self):
        self.repo.close()
        self.temp.cleanup()

    def feed(self, events):
        for e in events:
            self.processor.process_message(topic(e), json.dumps(e).encode())

    def test_demo_prices_curve_end_and_history(self):
        self.feed(self.events[:-1])
        state = self.state.get_station_state(self.station)
        self.assertEqual(state['price_per_kwh'], .65)
        self.assertEqual(state['active_session_count'], 1)
        self.assertEqual(len(self.state.get_session_curve(self.sid)), 28)
        self.assertAlmostEqual(self.state.get_active_sessions()[0]['energy_kwh'],
                               self.events[-2]['payload']['energy_kwh'])
        self.feed(self.events[-1:])
        self.assertFalse(self.state.get_station_state(self.station)['occupancy'])
        self.assertEqual(self.state.get_session_curve(self.sid), [])
        self.assertEqual(self.repo.get_historical_metrics()['completed_session_count'], 1)

    def test_duplicates_leave_both_projections_unchanged(self):
        self.feed(self.events[:-1])
        before = self.state.get_session_curve(self.sid)
        result = self.processor.process(topic(self.events[-2]), self.events[-2])
        self.assertFalse(result.stored)
        self.assertEqual(self.state.get_session_curve(self.sid), before)
        self.assertEqual(len(self.repo.get_session_meter_events(self.sid)), 28)

    def test_storage_failure_does_not_update_memory(self):
        sink = Mock()
        sink.has_event.return_value = False
        sink.store_event.side_effect = RuntimeError('disk failure')
        processor = EventProcessor('comp5339/ev', self.state, sink)
        with self.assertRaises(RuntimeError):
            processor.process(topic(self.events[0]), self.events[0])
        self.assertEqual(self.state.get_all_station_states(), [])

    def test_store_duplicate_race_does_not_update_state(self):
        sink = Mock()
        sink.has_event.return_value = False
        sink.store_event.return_value = False
        result = EventProcessor('comp5339/ev', self.state, sink).process(topic(self.events[0]), self.events[0])
        self.assertFalse(result.state_updated)
        self.assertEqual(self.state.get_all_station_states(), [])

    def test_malformed_messages_and_topic_rejected(self):
        for payload in [b'\xff', b'{', b'[]', b'{"a":1,"a":2}', b'{}']:
            with self.subTest(payload=payload), self.assertRaises(InvalidMessage):
                self.processor.process_message('comp5339/ev/x/meter', payload)
        with self.assertRaises(InvalidMessage):
            self.processor.process('wrong/topic', self.events[0])
        self.assertEqual(self.state.get_all_station_states(), [])

    def test_orphan_meter_rolls_back_without_state(self):
        meter = next(e for e in self.events if e['event_type'] == 'meter')
        with self.assertRaises(ValueError):
            self.processor.process(topic(meter), meter)
        self.assertFalse(self.repo.has_event(meter['event_id']))
        self.assertEqual(self.state.get_all_station_states(), [])

    def test_restart_recovers_curve_and_ignores_redelivery(self):
        self.feed(self.events[:-1])
        restored = StateStore()
        restored.restore(self.repo)
        self.assertEqual(restored.get_all_station_states(), self.state.get_all_station_states())
        self.assertEqual(restored.get_session_curve(self.sid), self.state.get_session_curve(self.sid))
        processor = EventProcessor('comp5339/ev', restored, self.repo)
        self.assertFalse(processor.process(topic(self.events[-2]), self.events[-2]).stored)
        processor.process(topic(self.events[-1]), self.events[-1])
        self.assertEqual(restored.get_current_metrics()['active_session_count'], 0)
        json.dumps(restored.get_all_station_states())

    def test_multi_connector_and_same_second_handover(self):
        self.feed(self.events[:-1])
        start = deepcopy(next(e for e in self.events if e['event_type'] == 'session_start'))
        start['event_id'] = 'evt_second_connector'
        start['payload']['session_id'] += '-second'
        old_connector = start['payload']['connector_id']
        start['payload']['connector_id'] = 1 if old_connector != 1 else 2
        self.processor.process(topic(start), start)
        self.assertEqual(self.state.get_station_state(self.station)['occupied_connector_count'], 2)
        handover = deepcopy(start)
        handover['event_id'] = 'evt_handover'
        handover['payload']['session_id'] += '-handover'
        handover['payload']['connector_id'] = old_connector
        handover['event_time'] = self.events[-1]['event_time']
        self.processor.process(topic(handover), handover)
        self.feed(self.events[-1:])
        active = self.state.get_active_sessions()
        self.assertEqual(len(active), 2)
        self.assertIn(handover['payload']['session_id'], [s['session_id'] for s in active])
        self.assertNotIn(self.sid, [s['session_id'] for s in active])

    def test_snapshots_cannot_mutate_internal_state(self):
        self.feed(self.events[:-1])
        self.state.get_active_sessions()[0]['energy_kwh'] = -1
        self.state.get_session_curve(self.sid)[0]['power_kw'] = -1
        self.state.get_station_state(self.station)['active_sessions'].clear()
        self.assertGreaterEqual(self.state.get_active_sessions()[0]['energy_kwh'], 0)
        self.assertGreaterEqual(self.state.get_session_curve(self.sid)[0]['power_kw'], 0)
        self.assertEqual(self.state.get_current_metrics()['active_session_count'], 1)

    def test_parallel_duplicate_delivery_is_serialised(self):
        e = self.events[0]
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: self.processor.process(topic(e), e), range(30)))
        self.assertEqual(sum(r.stored for r in results), 1)

    def test_subscriber_ack_after_success_and_malformed_rejection(self):
        service = SubscriptionService(json.loads((ROOT / 'A2/config/mqtt.json').read_text()), self.repo)
        client = Mock()
        client.ack.return_value = 0
        e = self.events[0]
        message = Mock(topic=topic(e), payload=json.dumps(e).encode(), qos=1, mid=1)
        service._on_message(client, None, message)
        self.assertTrue(self.repo.has_event(e['event_id']))
        client.ack.assert_called_once_with(1, 1)
        message.payload = b'bad json'
        service._on_message(client, None, message)
        self.assertEqual(service.rejected_messages, 1)
        self.assertFalse(service.failed.is_set())

    def test_subscriber_does_not_ack_storage_failure(self):
        service = SubscriptionService(json.loads((ROOT / 'A2/config/mqtt.json').read_text()), self.repo)
        service.processor.process_message = Mock(side_effect=RuntimeError('disk failure'))
        client = Mock()
        service._on_message(client, None, Mock(topic='test', payload=b'{}', qos=1, mid=1))
        client.ack.assert_not_called()
        self.assertTrue(service.failed.is_set())


if __name__ == '__main__':
    unittest.main()
