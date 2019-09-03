# Copyright 2017 FUJITSU LIMITED
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import sys

from oslo_config import cfg
from oslo_log import log

from monasca_persister import conf
from monasca_persister import version

CONF = conf.CONF
LOG = log.getLogger(__name__)

_CONF_LOADED = False


def _get_config_files():
    """Get the possible configuration files accepted by oslo.config

    This also includes the deprecated ones
    """
    # default files
    conf_files = cfg.find_config_files(project='monasca',
                                       prog='monasca-persister')
    # deprecated config files (only used if standard config files are not there)
    if len(conf_files) == 0:
        old_conf_files = cfg.find_config_files(project='monasca',
                                               prog='persister')
        if len(old_conf_files) > 0:
            LOG.warning('Found deprecated old location "{}" '
                        'of main configuration file'.format(old_conf_files))
            conf_files += old_conf_files
    return conf_files


def parse_args(description='Persists metrics & alarm history in TSDB'):
    global _CONF_LOADED
    if _CONF_LOADED:
        LOG.debug('Configuration has been already loaded')
        return

    log.set_defaults()
    log.register_options(CONF)

    CONF(prog=sys.argv[1:],
         project='monasca',
         version=version.version_str,
         default_config_files=_get_config_files(),
         description=description)

    log.setup(CONF,
              product_name='monasca-persister',
              version=version.version_str)

    conf.register_opts()

    _CONF_LOADED = True
