# Monasca Persister

A Monasca Persister written in Python.

Reads alarms and metrics from a Kafka queue and stores them in an InfluxDB
database.

## Deployment

Note that this document refers to the Python implementation of the persister.
For information regarding the Java implementation, see the README.md in the
root of this repository.

### Package Installation

Monasca Persister should be installed from PyPI via pip. Use your distribution
package manager, or other preferred method, to ensure that you have pip
installed, it is typically called python-pip.

e.g. For Debian/Ubuntu:
```
sudo apt-get install python-pip
```

Alternately, you may want to follow the official instructions available at:
https://pip.pypa.io/en/stable/installing/

Now, to install a particular released version:

```
sudo pip install monasca-persister==<version>
```

If using InfluxDB, the persister requires the necessary Python library
to be installed. This can also be achieved via pip:

```
sudo pip install influxdb
```

Alternatively, pip can be used to install the latest development version
or a specific revision. This requires you have git installed in addition
to pip.

```
sudo apt-get install git
sudo pip install git+https://opendev.org/openstack/monasca-persister@<revision>#egg=monasca-persister
```

The installation will not cause the persister to run - it should first
be configured.

### Environment

Using the persister requires that the following components of the Monasca
system are deployed and available:

* Kafka
* Zookeeper (Required by Kafka)
* InfluxDB

If running the persister as a daemon, it is good practice to create a
dedicated system user for the purpose, for example:

```
sudo groupadd --system monasca
sudo useradd --system --gid monasca mon-persister
```

Additionally, it is good practice to give the daemon a dedicated working
directory, in the event it ever needs to write any files.

```
sudo mkdir -p /var/lib/monasca-persister
sudo chown mon-persister:monasca /var/lib/monasca-persister
```

The persister will write a log file in a location which can be changed
via the configuration, but by default requires that a suitable directory
be created as follows:

```
sudo mkdir -p /var/log/monasca/persister
sudo chown mon-persister:monasca /var/log/monasca/persister
```

Make sure to allow the user who will be running the persister to have
write access to the log directory.

### Configuration

There is minimum amount of configuration which must be performed before
running the persister. A template configuration file is installed in the
default location of /etc/monasca/monasca-persister.conf.

Note that the configuration will contain authentication information for
the database being used, so depending on your environment it may be
desirable to inhibit read access except for the monasca-persister group.

```
sudo chown root:monasca /etc/monasca/monasca-persister.conf
sudo chmod 640 /etc/monasca/monasca-persister.conf
```

Most of the configuration options should be left at default, but at a
minimum, the following should be changed:
The default value for influxdb ssl and verify_ssl is False. Only add/change if your influxdb is using SSL.

```
[zookeeper]
uri = <host1>:<port1>,<host2>:<port2>,...

[kafka_alarm_history]
uri = <host1>:<port1>,<host2>:<port2>,...

[kafka_metrics]
uri = <host1>:<port1>,<host2>:<port2>,...

[influxdb]
database_name =
ip_address =
port =
user =
password =
ssl =
verify_ssl =
```

### Running

The installation does not provide scripts for starting the persister, it
is up to the user how the persister is run. To run the persister manually,
which may be useful for troubleshooting:

```
sudo -u mon-persister \
  monasca-persister \
  --config-file /etc/monasca/monasca-persister.conf
```

Note that it is important to deploy the daemon in a manner such that the daemon
will be restarted if it exits (fails). There are a number of situations in which
the persister will fail-fast, such as all Kafka endpoints becoming unreachable.
For an example of this, see the systemd deployment section below.

### Running (systemd)

To run the persister as a daemon, the following systemd unit file can be used
and placed in ``/etc/systemd/system/monasca-persister.service``:

```
[Unit]
Description=OpenStack Monasca Persister
Documentation=https://github.com/openstack/monasca-persister/monasca-persister/README.md
Requires=network.target remote-fs.target
After=network.target remote-fs.target
ConditionPathExists=/etc/monasca/monasca-persister.conf
ConditionPathExists=/var/lib/monasca-persister
ConditionPathExists=/var/log/monasca/persister

[Service]
Type=simple
PIDFile=/var/run/monasca-persister.pid
User=mon-persister
Group=monasca
WorkingDirectory=/var/lib/monasca-persister
ExecStart=/usr/local/bin/monasca-persister --config-file /etc/monasca/monasca-persister.conf
Restart=on-failure
RestartSec=5
SyslogIdentifier=monasca-persister

[Install]
WantedBy=multi-user.target
```

After creating or modifying the service file, you should run:

```
sudo systemctl daemon-reload
```

The service can then be managed as normal, e.g.

```
sudo systemctl start monasca-persister
```

For a production deployment, it will almost always be desirable to use the
*Restart* clause in the service file. The *RestartSec* clause is also
important as by default, systemd assumes a delay of 100ms. A number of
failures in quick succession will cause the unit to enter a failed state,
therefore extending this period is critical.


# License

(C) Copyright 2014-2016 Hewlett Packard Enterprise Development Company LP

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied.
See the License for the specific language governing permissions and
limitations under the License.
