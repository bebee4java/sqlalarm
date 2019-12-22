## SQLAlarm
> Big data smart alarm by sql

SQLAlarm is for event alarm which is built on spark structured-steaming. This system including following abilities:
1. Event filtering through SQL
2. Alarm record noise reduction
3. Alarm record dispatch in specified channels

The integral framework idea is as follows:
![sqlalarm](docs/sqlalarm.png)

Introduce of modules:
1. sa-admin: web console and rest api for sqlalarm
2. sa-core: core module of sqlalarm