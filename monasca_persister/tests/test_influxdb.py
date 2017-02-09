# -*- coding: utf-8 -*-
# (C) Copyright 2017 Hewlett Packard Enterprise Development LP
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

from oslotest import base

from monasca_persister.repositories.influxdb import line_utils

class TestInfluxdb(base.BaseTestCase):

    def setUp(self):
        super(TestInfluxdb, self).setUp()

    def tearDown(self):
        super(TestInfluxdb, self).tearDown()

    def test_line_utils_handles_utf8(self):
        utf8_name = u'ｎａｍｅ'
        self.assertEqual(u'"' + utf8_name + u'"', line_utils.escape_value(utf8_name))
        self.assertEqual(utf8_name, line_utils.escape_tag(utf8_name))

    def test_line_utils_escape_tag(self):
        simple = u"aaaaa"
        self.assertEqual(simple, line_utils.escape_tag(simple))
        complex = u"a\\ b,c="
        self.assertEqual("a\\\\\\ b\\,c\\=", line_utils.escape_tag(complex))

    def test_line_utils_escape_value(self):
        simple = u"aaaaa"
        self.assertEqual(u'"' + simple + u'"', line_utils.escape_value(simple))
        complex = u"a\\b\"\n"
        self.assertEqual(u"\"a\\\\b\\\"\\n\"", line_utils.escape_value(complex))
