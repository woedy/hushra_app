from types import SimpleNamespace
from unittest.mock import patch

from django.urls import reverse
from rest_framework.test import APITestCase

from .models import HushraCredentials, SearchTask


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
