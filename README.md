# gocom
ADL Indo golang common library. Provide basic feature for develops api server such as HTTP server, db, key/value, queue and pubsub

# How to use pubsub
pupsub is a library in Gocom for publishing and subscribing messages between services

In the Gocom library there are two types of pubsub types:

        1. nats.io
        2. kafka

###    1.1 how to use nats.io
You only need to do your configuration 
in the config.properties file.

    example for nats.oi
    app.pubsub.default.type=nats
    # for local env
    app.pubsub.default.url=nats://127.0.0.1:4222

###    1.2 how to use kafka
Same with nats.io you just need to do your configuration
in the config.properties file.

    example for kafka
    app.pubsub.default.type=kafka
    # for env local 
    app.pubsub.default.url=bootstrap.servers=localhost:9092;security.protocol=PLAINTEXT
    # for env dev
    app.pubsub.default.url=bootstrap.servers=<host>:<port>;security.protocol=SASL_SSL;sasl.mechanism=SCRAM-SHA-512;sasl.username=<username>;sasl.password=<password>
