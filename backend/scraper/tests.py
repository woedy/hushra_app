from types import SimpleNamespace
from unittest.mock import patch

from django.urls import reverse
from rest_framework.test import APITestCase

from .models import HushraCredentials, SearchTask
from .models import SearchJob, StateRun, PersonRecord
from .models import GlobalSetting
from .tasks import _select_state_iteration_order, generate_uuids_task
from .hushra_client import HushraAPIClient


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

    def test_state_run_records_endpoint_returns_paginated_data(self):
        task = SearchTask.objects.create(
            job=self.job,
            state_run=self.state_run,
            state='CA',
            axis='lastname',
            lastname='a',
            status='COMPLETED'
        )
        PersonRecord.objects.bulk_create([
            PersonRecord(task=task, firstname=f'John{i}', lastname='Doe', ssn=f'999-99-{1000+i}')
            for i in range(30)
        ])

        url = reverse('staterun-records', args=[self.state_run.id])
        response = self.client.get(url, {'page': 2, 'page_size': 10})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload['count'], 30)
        self.assertEqual(payload['page'], 2)
        self.assertEqual(payload['page_size'], 10)
        self.assertEqual(len(payload['results']), 10)

    def test_general_records_pagination(self):
        # Create a dummy job and task as they are required by PersonRecord
        job = SearchJob.objects.create(name='Test Job')
        task = SearchTask.objects.create(job=job, state='CA', status='COMPLETED')
        
        # Create some records associated with the task
        PersonRecord.objects.bulk_create([
            PersonRecord(task=task, firstname=f'John{i}', lastname='Doe', ssn=f'000-00-{2000+i}')
            for i in range(150)
        ])

        url = reverse('personrecord-list')
        response = self.client.get(url, {'page': 1})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        
        # Verify DRF pagination structure
        self.assertIn('count', payload)
        self.assertIn('results', payload)
        self.assertEqual(payload['count'], 150)
        self.assertEqual(len(payload['results']), 100) # Default page size is 100

        # Verify second page
        response = self.client.get(url, {'page': 2})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(len(payload['results']), 50)

    def test_generate_uuids_endpoint(self):
        url = reverse('hushracredentials-generate')
        response = self.client.post(url, {'count': 5})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['count'], 5)
        self.assertEqual(response.json()['status'], 'Generation started')

    def test_register_account_logic(self):
        # Mocking the hushra_client method instead of making real network calls
        from unittest.mock import patch
        with patch('scraper.hushra_client.curl_requests.post') as mock_post:
            mock_post.return_value.status_code = 201
            mock_post.return_value.json.return_value = {'uuid': 'mocked-uuid-123'}
            
            client = HushraAPIClient()
            result = client.register_account('testuser')
            
            self.assertIsNotNone(result)
            self.assertEqual(result['uuid'], 'mocked-uuid-123')

    def test_generate_uuids_task_logic(self):
        from unittest.mock import patch
        with patch('scraper.tasks.HushraAPIClient.register_account') as mock_register:
            mock_register.return_value = {'uuid': 'task-generated-uuid'}
            
            # Run task synchronously for testing
            from .tasks import generate_uuids_task
            generate_uuids_task(count=1)
            
            # Verify one credential was created
            self.assertTrue(HushraCredentials.objects.filter(uuid='task-generated-uuid').exists())


class StateOrderingTests(APITestCase):
    def test_round_robin_state_order_rotates_each_tick(self):
        GlobalSetting.objects.update_or_create(key='auto_state_order_mode', defaults={'value': 'round_robin'})
        GlobalSetting.objects.update_or_create(key='auto_state_cursor', defaults={'value': '0'})

        states = ['CA', 'NY', 'TX']
        first = _select_state_iteration_order(states)
        second = _select_state_iteration_order(states)

        self.assertEqual(first, ['CA', 'NY', 'TX'])
        self.assertEqual(second, ['NY', 'TX', 'CA'])

    def test_random_state_order_keeps_same_members(self):
        GlobalSetting.objects.update_or_create(key='auto_state_order_mode', defaults={'value': 'random'})
        states = ['CA', 'NY', 'TX']
        ordered = _select_state_iteration_order(states)
        self.assertEqual(set(ordered), {'CA', 'NY', 'TX'})
        self.assertEqual(len(ordered), 3)
