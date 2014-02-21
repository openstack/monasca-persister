mon-persister
=============

Monitoring persister to read metrics and related messages from Kafka and write to persistent data store


Basic design is to have one kafka consumer feed a disruptor that has vertica output processors.
