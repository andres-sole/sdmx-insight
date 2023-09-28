# SDMX Insight

![SDMX Insight Logo](superset-frontend/src/assets/images/logo.png)

SDMX Insight is a fork of [Apache Superset](https://github.com/apache/superset). It's an open-source project initially created for the [SDMX 2023 Hackathon](https://www.sdmx2023.org/hackathon) with the objective of creating a dashboard generator.

## Technologies

SDMX Insight is primarily built using, among others:
  - ReactJS
  - Flask
  - SDMXthon
  - Celery
  - Redis

## Deployment

### Development Mode
To run the project in development mode on Linux systems, execute the following command:

```bash
docker-compose up
```


## Production Mode

For running the project in production mode on Linux systems, use:

```bash
docker-compose -f docker-compose-non-dev.yml up -d
```

Note: The project can also be launched on Windows by running the aforementioned compose files via Docker Desktop.

