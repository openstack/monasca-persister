mon-persister
=============

The Monitoring Persister consumes metrics and alarm state transitions from the Message Queue and stores them in the Metrics and Alarms database.

The Persister uses the DropWizard which provides a nice application framework. Although the Persister isn't primarly a Web service, it exposes an http endpoint that provides an interface through which metrics about the Persister can be queried as well as health status. 

The basic design of the Persister is to have one Kafka consumer feeds a Disruptor, https://github.com/LMAX-Exchange/disruptor, that has output processors that does batch writes to the Metrics and Alarms database. 

# TODO

* Purge metrics on shutdown
* Add more robust offset management.
* Add better handlign of SQL exceptions
* Complete health check
* Specify the names of the metrics that are available for monitoring.

# License

Copyright (c) 2014 Hewlett-Packard Development Company, L.P.

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
