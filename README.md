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

By cloning this repository, SDMX Insight can be deployed on any system that supports the Docker  environment.



### Production Mode

For running the project in production mode on Linux systems, use:

```bash
docker-compose -f docker-compose-non-dev.yml up -d
```

On Windows:

```powershell
docker compose -f docker-compose-non-dev.yml up -d
```

### Development Mode
To run the project in development mode on Linux systems, execute the following command:

```bash
docker-compose up
```
On Windows:

```powershell
docker compose up
```

### Additional notes

After deployment, SDMX Insight will be accessible at localhost:8088. For custom domain access, set up a reverse proxy using a web server like NGINX or Apache HTTP Server.

A default admin user is generated with the username "admin" and the password "admin". This can be modified afterwards.

The port to which SDMX Insight is deployed can be altered by adjusting the port mapping in docker-compose-non-dev.yml or docker-compose.yml depending on the configuration you are running.

## Dashboard YAML

During the 2023 SDMX Hackathon, SDMX Insight partially implemented a YAML specification provided by the organizers. You can check the status of this implementation [here](specification_status.md)