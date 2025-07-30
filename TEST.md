```toml
# fly.toml app configuration file generated for crawlagent on 2025-01-16T16:53:43+02:00
#
# See https://fly.io/docs/reference/configuration/ for information about how to use this file.
#

app = 'crawlagent'
primary_region = 'fra'

[build]
[env]
#CELERY_BROKER_URL = "amqp://guest:guest@localhost:5672//"
#RAY_ADDRESS = "ray://localhost:6379"
UPSTASH_REDIS_REST_URL = "https://gusc1-saved-terrapin-30766.upstash.io"

[http_service]
internal_port = 8000
force_https = true
auto_stop_machines = 'stop'
auto_start_machines = true
min_machines_running = 1
max_machines_running = 3

processes = ['app']

[[vm]]
memory = '1gb'
cpu_kind = 'shared'
cpus = 4

```
