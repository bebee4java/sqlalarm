## SQLAlarm
> Big data smart alarm by sql

SQLAlarm is for event(time-stamped) alarm which is built on spark structured-steaming. This system including following abilities:
1. Event filtering through SQL
2. Alarm records noise reduction
3. Alarm records dispatch in specified channels

The integral framework idea is as follows:
![sqlalarm](docs/sqlalarm.png)

Introduce of modules:
1. sa-admin: web console and rest api for sqlalarm
2. sa-core: core module of sqlalarm(including source/filter/sink(alert))

### Developing SQLAlarm
You can use bin/start-local.sh to start a local SQLAlarm serve at IntelliJ IDEA. We recommend to run it use yarn-client or local mode in spark cluster after packaged jar.

Minimal requirements for a SQLAlarm serve are:
- Java 1.8 + 
- Spark 2.4.x
- Redis (Redis 5.0, if use Redis Stream)
- Kafka (this is also needless if you only use Redis Stream for event alerts)

For example, I started a SQLAlarm serve that consume kafka event message to do alarm flow:
```bash
spark-submit --class dt.sql.alarm.SQLAlarmBoot \
        --driver-memory 2g \
        --master local[4] \
        --name SQLALARM \
        --conf "spark.kryoserializer.buffer=256k" \
        --conf "spark.kryoserializer.buffer.max=1024m" \
        --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
        --conf "spark.redis.host=127.0.0.1" \
        --conf "spark.redis.port=6379" \
        --conf "spark.redis.db=4" \
        sa-core-1.0-SNAPSHOT.jar \
        -sqlalarm.name sqlalarm \
        -sqlalarm.sources kafka \
        -sqlalarm.input.kafka.topic sqlalarm_event \
        -sqlalarm.input.kafka.subscribe.topic.pattern 1 \
        -sqlalarm.input.kafka.bootstrap.servers "127.0.0.1:9092" \
        -sqlalarm.sinks console
```
> notes: the above simple example takes kafka as the message center, filtering alarm event and output to the console.

### Quick Start
1. Packaged the core jar: sa-core-1.0-SNAPSHOT.jar.
2. Deploy the jar package in spark cluster.
3. Add an alarm rule(put at redis): 
```bash
# hset key uuid value
# key: sqlalarm_rule:${sourceType}:${topic}

HSET "sqlalarm_rule:kafka:sqlalarm_event" "uuid00000001" 
{
    "item_id":"uuid00000001",
    "platform":"alarm",
    "title":"sql alarm test",
    "source":{
        "type":"kafka",
        "topic":"sqlalarm_event"
    },
    "filter":{
        "table":"fail_job",
        "structure":[
            {
                "name":"job_name",
                "type":"string",
                "xpath":"$.job_name"
            },
            {
                "name":"job_owner",
                "type":"string",
                "xpath":"$.job_owner"
            },
            {
                "name":"job_stat",
                "type":"string",
                "xpath":"$.job_stat"
            },
            {
                "name":"job_time",
                "type":"string",
                "xpath":"$.job_time"
            }
        ],
        "sql":"select job_name as job_id,job_stat,job_time as event_time,'job failed' as message, map('job_owner',job_owner) as context from fail_job where job_stat='Fail'"
    }
}
```
4. Wait for event center(may be kafka or redis) produce alarm events. Produce manually:
> 1.create if not exists topic: 
```bash
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sqlalarm_event
```
> 2.produce events: 
```bash
kafka-console-producer.sh --broker-list localhost:9092 --topic sqlalarm_event

{
    "job_name":"sqlalarm_job_000",
    "job_owner":"bebee4java",
    "job_stat":"Succeed",
    "job_time":"2019-12-26 12:00:00"
}

{
    "job_name":"sqlalarm_job_001",
    "job_owner":"bebee4java",
    "job_stat":"Fail",
    "job_time":"2019-12-26 12:00:00"
}
```
5. If use console sink, you will get following info in the console(Observable the fail events are filtered out and succeed events are ignored):
![alarm-console-sink](docs/alarm-console-sink.jpg)

> **notes:** the order of step 2&3 is not required, and the alarm rule is not necessary when starting the SQLAlarm serve.
 
### Features
1. Supports docking multiple data sources as event center(kafka or redis stream-enabled source), and it's scalable you can customize the data source only extends the class [BaseInput](sa-core/src/main/java/dt/sql/alarm/input/BaseInput.scala)
2. Supports docking multiple data topics with inconsistent structure
3. Supports output of alarm events to multiple sinks(kafka/jdbc/es etc.), and it's scalable you can customize the data sink only extends the class [BaseOutput](sa-core/src/main/java/dt/sql/alarm/output/BaseOutput.scala)
4. Supports alarm filtering for events through SQL
5. Supports multiple policies(time merge/time window+N counts merge) for alarm noise reduction
6. Supports alarm rules and policies to take effect dynamically without restarting the serve
7. Supports adding data source topics dynamically(If your subscription mode is `subscribePattern`)
8. Supports sending alarm records by specific different channels

### Collectors
SQLAlarm does't automatically generate metrics events, it only obtains metrics events from the message center and analyzes them.
However, you can collect and report metrics events in another project called [metrics-exporter](https://github.com/bebee4java/metrics-exporter), 
this makes up for this shortage well. 

**In this way, a complete alarm process looks like:  
[metrics-exporter](https://github.com/bebee4java/metrics-exporter) —> [sqlalarm](https://github.com/bebee4java/sqlalarm) —> alarm-pigeon**

### Documentation
The documentation of SQLAlarm is located on the issues page: [SQLAlarm issues](https://github.com/bebee4java/sqlalarm/issues).
It contains a lot of information such as [configuration](https://github.com/bebee4java/sqlalarm/issues/2) and use tutorial etc. If you have any questions, please free to commit issues.

### Fork and Contribute
This is an active open-source project. We are always open to people who want to use the system or contribute to it. Contact us if you are looking for implementation tasks that fit your skills.
