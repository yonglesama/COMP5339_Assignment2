"""Real loopback MQTT -> B -> C test; uses a temporary database and broker."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'A2/src'))
import paho.mqtt.client as mqtt
from database.repository import EventRepository
from database.initialise import initialise_database
from publishing.run import publish_events
from subscription.run import SubscriptionService


def wait_until(predicate, timeout=10):
    end = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= end:
            raise TimeoutError('MQTT integration condition not met')
        time.sleep(.02)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report', type=Path)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as directory:
        work = Path(directory)
        source_database = work / 'A1/data/final/ev_chargers.duckdb'
        schema = work / 'A2/sql/a2_schema.sql'
        mapping = work / 'A2/data/input/station_connector_mapping.csv'
        source_database.parent.mkdir(parents=True)
        schema.parent.mkdir(parents=True)
        mapping.parent.mkdir(parents=True)
        shutil.copy2(ROOT / 'A1/data/final/ev_chargers.duckdb', source_database)
        shutil.copy2(ROOT / 'A2/sql/a2_schema.sql', schema)
        shutil.copy2(ROOT / 'A2/data/input/station_connector_mapping.csv', mapping)
        database = initialise_database(work)
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            port = probe.getsockname()[1]
        log = (work / 'broker.log').open('w')
        broker = subprocess.Popen([sys.executable, str(ROOT / 'A2/src/publishing/local_broker.py'),
                                   '--port', str(port)], stdout=log, stderr=subprocess.STDOUT)
        repository = subscriber = publisher = None
        try:
            def listening():
                if broker.poll() is not None:
                    raise RuntimeError((work / 'broker.log').read_text())
                try:
                    with socket.create_connection(('127.0.0.1', port), timeout=.1):
                        return True
                except OSError:
                    return False
            wait_until(listening)
            config = json.loads((ROOT / 'A2/config/mqtt.json').read_text())
            config.update(host='127.0.0.1', port=port)
            config['client_ids']['subscriber'] = 'member-b-integration'
            repository = EventRepository(database)
            assert repository.get_historical_metrics()['completed_session_count'] == 0
            events = [json.loads(s) for s in (ROOT / 'A2/data/demo/event_stream.jsonl').read_text().splitlines()]
            subscriber = SubscriptionService(config, repository)
            subscriber.start()
            publisher = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id='member-b-test-publisher')
            publisher.connect('127.0.0.1', port)
            publisher.loop_start()
            wait_until(publisher.is_connected)
            first = publish_events(events[:10], publisher, config, delay=.1)
            wait_until(lambda: subscriber.stored_messages == 10 or subscriber.failed.is_set())
            assert not subscriber.failed.is_set(), subscriber.error
            # Locate the session without assuming the initial price count.
            sid = next(e['payload']['session_id'] for e in events if e['event_type'] == 'session_start')
            curve_count = len(subscriber.state_store.get_session_curve(sid))
            assert curve_count > 0
            subscriber.stop()
            subscriber = SubscriptionService(config, repository)
            subscriber.start()
            assert len(subscriber.state_store.get_session_curve(sid)) == curve_count
            assert subscriber.state_store.get_current_metrics()['active_session_count'] == 1
            second = publish_events(events[10:], publisher, config, delay=.1)
            wait_until(lambda: subscriber.stored_messages == len(events)-10 or subscriber.failed.is_set())
            assert not subscriber.failed.is_set(), subscriber.error
            assert subscriber.state_store.get_current_metrics()['active_session_count'] == 0
            assert repository.get_historical_metrics()['completed_session_count'] == 1
            publish_events(events, publisher, config, delay=.1)
            wait_until(lambda: subscriber.duplicate_messages == len(events) or subscriber.failed.is_set())
            assert not subscriber.failed.is_set(), subscriber.error
            assert repository.get_historical_metrics()['completed_session_count'] == 1
            bad = publisher.publish('comp5339/ev/bad/meter', b'{', qos=1)
            bad.wait_for_publish(timeout=5)
            wait_until(lambda: subscriber.rejected_messages == 1)
            report = {'passed': True, 'transport': 'real loopback MQTT QoS 1',
                      'new_events_stored': len(events), 'duplicate_events_ignored': subscriber.duplicate_messages,
                      'malformed_messages_rejected': subscriber.rejected_messages,
                      'restored_curve_points': curve_count,
                      'final_active_sessions': subscriber.state_store.get_current_metrics()['active_session_count'],
                      'historical_metrics': repository.get_historical_metrics(),
                      'minimum_send_gap_seconds': min(first['minimum_send_gap_seconds'], second['minimum_send_gap_seconds']),
                      'scope': f'{len(events)}-event demo, temporary DuckDB; not full assessment replay'}
            print(json.dumps(report, indent=2))
            if args.report:
                args.report.parent.mkdir(parents=True, exist_ok=True)
                args.report.write_text(json.dumps(report, indent=2)+'\n')
        finally:
            if subscriber:
                subscriber.stop()
            if publisher:
                publisher.disconnect()
                publisher.loop_stop()
            if repository:
                repository.close()
            broker.terminate()
            try:
                broker.wait(timeout=5)
            except subprocess.TimeoutExpired:
                broker.kill()
                broker.wait()
            log.close()


if __name__ == '__main__':
    main()
