# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from mock import patch
from mock import call
from mock import Mock
import signal

from oslo_config import cfg
from oslotest import base

CONF = cfg.CONF

NUMBER_OF_METRICS_PROCESSES = 2
NUMBER_OF_ALARM_HIST_PROCESSES = 3


class FakeException(Exception):
    pass


class TestPersister(base.BaseTestCase):

    def setUp(self):
        super(TestPersister, self).setUp()

        from monasca_persister import persister
        self.persister = persister

        self._set_patchers()
        self._set_mocks()

    def _set_patchers(self):
        self.sys_exit_patcher = patch.object(self.persister.sys, 'exit')
        self.log_patcher = patch.object(self.persister, 'log')
        self.simport_patcher = patch.object(self.persister, 'simport')
        self.cfg_patcher = patch.object(self.persister, 'cfg')
        self.sleep_patcher = patch.object(self.persister.time, 'sleep')
        self.process_patcher = patch.object(self.persister.multiprocessing, 'Process')

    def _set_mocks(self):
        self.mock_sys_exit = self.sys_exit_patcher.start()
        self.mock_log = self.log_patcher.start()
        self.mock_simport = self.simport_patcher.start()
        self.mock_cfg = self.cfg_patcher.start()
        self.mock_sleep = self.sleep_patcher.start()
        self.mock_process_class = self.process_patcher.start()

        self.mock_cfg.CONF.kafka_metrics.num_processors = NUMBER_OF_METRICS_PROCESSES
        self.mock_cfg.CONF.kafka_alarm_history.num_processors = NUMBER_OF_ALARM_HIST_PROCESSES
        self.mock_cfg.CONF.zookeeper = 'zookeeper'

        self.mock_sleep.side_effect = [FakeException, None]

        self.process_pool = _get_process_list(
            NUMBER_OF_METRICS_PROCESSES + NUMBER_OF_ALARM_HIST_PROCESSES + 1)
        self.mock_process_class.side_effect = self.process_pool

    def tearDown(self):
        super(TestPersister, self).tearDown()

        self.mock_sleep.side_effect = None

        self.mock_sys_exit.side_effect = None
        self.mock_sys_exit.reset_mock()

        self.sys_exit_patcher.stop()
        self.log_patcher.stop()
        self.simport_patcher.stop()
        self.cfg_patcher.stop()
        self.sleep_patcher.stop()
        self.process_patcher.stop()

        self.persister.processors = []
        self.persister.exiting = False

    def _run_persister(self):
        self.persister.main()
        self.assertTrue(self.mock_process_class.called)

    def _assert_clean_exit_handler_terminates_with_expected_signal(
            self, signum, expected):
        # in order to really exit, an exception is thrown

        self.mock_sys_exit.side_effect = FakeException
        self.assertRaises(FakeException, self.persister.clean_exit, signum)

        self.mock_sys_exit.assert_called_once_with(expected)

    def _assert_correct_number_of_processes_created(self):

        for p in self.process_pool[:-1]:
            self.assertTrue(p.start.called)

        self.assertFalse(self.process_pool[-1].called)

    def _assert_process_alive_status_called(self):

        for p in self.process_pool[:-1]:
            self.assertTrue(p.is_alive.called)

    def _assert_process_terminate_called(self):

        for p in self.process_pool[:-1]:
            self.assertTrue(p.terminate.called)

    def test_active_children_are_killed_during_exit(self):

        with patch.object(self.persister.multiprocessing, 'active_children') as active_children,\
             patch.object(self.persister.os, 'kill') as mock_kill:

            active_children.return_value = [Mock(name='child-1', pid=1),
                                            Mock(name='child-2', pid=2)]

            self.persister.clean_exit(0)

            mock_kill.assert_has_calls([call(1, signal.SIGKILL), call(2, signal.SIGKILL)])

    def test_active_children_kill_exception_is_ignored(self):

        with patch.object(self.persister.multiprocessing,
                          'active_children') as active_children, \
                patch.object(self.persister.os, 'kill') as mock_kill:

            active_children.return_value = [Mock()]
            mock_kill.side_effect = FakeException

            self.persister.clean_exit(0)

            self.assertTrue(mock_kill.called)

    def test_clean_exit_does_nothing_when_exiting_is_true(self):
        self.persister.exiting = True

        self.assertIsNone(self.persister.clean_exit(0))

        self.assertFalse(self.mock_sys_exit.called)

    def test_exception_during_process_termination_is_ignored(self):
        dead_process = _get_process('raises_when_terminated')
        dead_process.terminate.side_effect = FakeException
        self.process_pool[0] = dead_process

        self._run_persister()

        self.assertTrue(dead_process.terminate.called)

    def test_if_not_sigterm_then_clean_exit_handler_terminates_with_signal(self):

        self._assert_clean_exit_handler_terminates_with_expected_signal(
            signal.SIGINT, signal.SIGINT)

    def test_if_sigterm_then_clean_exit_handler_terminates_with_zero(self):

        self._assert_clean_exit_handler_terminates_with_expected_signal(
            signal.SIGTERM, 0)

    def test_non_running_process_not_terminated(self):
        dead_process = _get_process('dead_process', is_alive=False)
        self.process_pool[0] = dead_process

        self._run_persister()

        self.assertTrue(dead_process.is_alive.called)
        self.assertFalse(dead_process.terminate.called)

    def test_running_processes_are_created_and_terminated(self):

        self._run_persister()

        self._assert_correct_number_of_processes_created()
        self._assert_process_alive_status_called()
        self._assert_process_terminate_called()

    def test_start_process_handler_creates_and_runs_persister(self):
        fake_kafka_config = Mock()
        fake_repository = Mock()

        with patch('monasca_persister.repositories.persister.Persister') as mock_persister_class:
            self.persister.start_process(fake_repository, fake_kafka_config)

            mock_persister_class.assert_called_once_with(
                fake_kafka_config, 'zookeeper', fake_repository)


def _get_process(name, is_alive=True):
    return Mock(name=name, is_alive=Mock(return_value=is_alive))


def _get_process_list(number_of_processes):
    processes = []
    for i in range(number_of_processes):
        processes.append(_get_process(name='process_{}'.format(i)))
    return processes
