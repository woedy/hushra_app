from types import SimpleNamespace
from unittest.mock import patch

from django.urls import reverse
from rest_framework.test import APITestCase

from .models import HushraCredentials, SearchTask
from .models import SearchJob, StateRun


class SmokeFlowTests(APITestCase):
    def setUp(self):
        HushraCredentials.objects.create(uuid='test-uuid-1', is_active=True)

    @patch('scraper.views.execute_ssn_lookup.delay')
    def test_smoke_test_seeds_tasks(self, mocked_delay):
        mocked_delay.side_effect = [SimpleNamespace(id=f'task-{i}') for i in range(10)]

        url = reverse('globalsetting-smoke-test')
        response = self.client.post(
            url,
            {
                'state': 'CA',
                'axis': 'lastname',
                'count': 3,
                'clear_existing': True,
            },
            format='json',
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(SearchTask.objects.count(), 3)
        self.assertEqual(SearchTask.objects.filter(status='PENDING').count(), 3)

    def test_worker_health_returns_counts(self):
        url = reverse('globalsetting-worker-health')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertIn('credentials', body)
        self.assertIn('tasks', body)


class ControlFlowTests(APITestCase):
    def setUp(self):
        HushraCredentials.objects.create(uuid='test-uuid-control', is_active=True)
        self.job = SearchJob.objects.create(name='Auto Orchestrator Job', status='RUNNING')
        self.state_run = StateRun.objects.create(job=self.job, state='CA', status='RUNNING', axes_enabled=['lastname'])

    @patch('core.celery.app.control.revoke')
    @patch('core.celery.app.connection_or_acquire')
    def test_new_session_disables_auto_run_and_cancels_active_tasks(self, mocked_conn, mocked_revoke):
        t1 = SearchTask.objects.create(job=self.job, state_run=self.state_run, state='CA', axis='lastname', lastname='a', status='PENDING', celery_task_id='tid-1')
        t2 = SearchTask.objects.create(job=self.job, state_run=self.state_run, state='CA', axis='lastname', lastname='b', status='IN_PROGRESS', celery_task_id='tid-2')

        class DummyConn:
            class DummyChannel:
                def queue_purge(self, *_args, **_kwargs):
                    return 0
            default_channel = DummyChannel()
            def __enter__(self):
                return self
            def __exit__(self, exc_type, exc, tb):
                return False

        mocked_conn.return_value = DummyConn()

        url = reverse('globalsetting-new-session')
        response = self.client.post(url, {}, format='json')
        self.assertEqual(response.status_code, 200)

        self.state_run.refresh_from_db()

        self.assertEqual(self.state_run.status, 'PAUSED')
        self.assertEqual(SearchTask.objects.filter(id__in=[t1.id, t2.id]).count(), 0)
        self.assertGreaterEqual(mocked_revoke.call_count, 1)

    @patch('core.celery.app.control.revoke')
    def test_bulk_control_stop_pauses_and_stops_tasks(self, mocked_revoke):
        SearchTask.objects.create(job=self.job, state_run=self.state_run, state='CA', axis='lastname', lastname='a', status='IN_PROGRESS', celery_task_id='tid-bulk')

        url = reverse('staterun-bulk-control')
        response = self.client.post(url, {'action': 'stop', 'state_run_ids': [self.state_run.id]}, format='json')
        self.assertEqual(response.status_code, 200)

        self.state_run.refresh_from_db()
        self.assertEqual(self.state_run.status, 'PAUSED')
        self.assertEqual(SearchTask.objects.filter(state_run=self.state_run, status='STOPPED').count(), 1)
        mocked_revoke.assert_called_once()
