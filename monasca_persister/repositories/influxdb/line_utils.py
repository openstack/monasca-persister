# (C) Copyright 2017 Hewlett Packard Enterprise Development LP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from six import PY2


def escape_tag(tag):
    tag = get_unicode(tag)
    return tag.replace(
        u"\\", u"\\\\"
    ).replace(
        u" ", u"\\ "
    ).replace(
        u",", u"\\,"
    ).replace(
        u"=", u"\\="
    )

def get_unicode(data):
    if PY2:
        return unicode(data)
    else:
        return str(data)

def escape_value(value):
    return u"\"{0}\"".format(
        get_unicode(value).replace(
            u"\\", u"\\\\"
        ).replace(
            u"\"", u"\\\""
        ).replace(
            u"\n", u"\\n"
        )
    )
