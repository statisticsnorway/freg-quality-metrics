# freg-quality-metrics

## Tech stack

* Python 3.9
* Poetry for package management
* FastAPI for api's


The endpoints exposed from this simple app are:

* <http://localhost:8080/docs>
* <http://localhost:8080/metrics>
* <http://localhost:8080/health/alive>
* <http://localhost:8080/health/ready>

## Local development

When running and testing with Docker locally, uncomment line 54-56 in the Dockerfile.
Create a key for the service account `data-quality@dev-freg-3896.iam.gserviceaccount.com`
and store it as `service-key-dev.json` in the repo root directory.

Then:
```shell
docker build -t freg-metrics .
docker run --rm -p 8080:8080 freg-metrics
```
And see the result at <http://localhost:8080/metrics>

