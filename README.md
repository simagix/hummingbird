# The Hummingbird Project
As a result of "losing" my [MongoPush](https://www.simagix.com/2021/12/a-series-of-mongopush-events.html) source code, I rewrote it but on a smaller scale.  I couldn't get much meaningful help in my previous close source project.  To support many use cases is too big an effort for a person to take on, especially the oplogs streaming part which is in fact a reverse engineering of replication.  I still think it is a great idea and many can benefit from it.  So, here you go, an open source project.  Contributions are welcome.

The idea of the repository name was from the movie [The Hummingbird Project (2018)](https://en.wikipedia.org/wiki/The_Hummingbird_Project).  The world can use good ideas.  Birds can't fly over the ocean, not because they lack courage, but because there is no one waiting on the other side.

## Quick Start
### Configuration File Example
```bash
{
  "command": "all",
  "drop": true,
  "source": "mongodb://user:password@source@example.com/?compressors=zstd,snappy&readPreference=secondaryPreferred",
  "target": "mongodb+srv://user:password@target.example.mongodb.net/?compressors=zstd,snappy&w=2&retryWrites=true",
  "license": "Apache-2.0"
}
```

### Start Migration
- Start neutrino
```bash
go run main/hummingbird.go -start configuration.json
```
- Add Additional Workers
```bash
go run main/hummingbird.go -worker configuration.json
```

### Progress Monitoring
http://localhost:3629

## Build
```bash
./build.sh
```

## Configurations
```json
{
  "block": 10000,
  "command": "all|config|index|data|data-only",
  "drop": false,
  "includes": [
    {
      "namespace": "database.collection",
      "filter": {},
      "to": "database.collection",
      "limit": 0,
      "masks": ["field"],
      "method": "default|hex|partial"
    }
  ],
  "license": "Apache-2.0",
  "port": 3629,
  "source": "mongodb://[user:XXXXXX@]host[:port][/[db][?options]]",
  "spool": "./spool",
  "target": "mongodb+srv://user:XXXXXX@host[/[db][?options]]",
  "verbose": false,
  "workers": 8,
  "yes": false
}
```

## License
[Apache-2.0 License](https://www.apache.org/licenses/LICENSE-2.0)