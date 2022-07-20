# ssb-python-microservice-template

## This is...

A template repository to jumpstart the creation of a Python microservice using FastAPI targeted for Kubernetes.
The goal is to provide some "boiler-plate" config to speed up the process from creating a new repository to
deploying the app "Hello world"-style

## This is not...

A developer guide on Python or exhaustive documentation on BIP. For the latter,
please head on over to the [SSB developer guide](https://docs.bip.ssb.no)

## Included in the template

* Makefile for configuring the environment,running the app and configuring CI-definition
and deployment descriptor. Run `make` at root to display options
* Logging configuration (which also works in Google Logging)
* Metrics configuration
* Simple unit-tests
* The possibility for creating a `pre-commit-hook` to enforce code formatting using Black
* Pycrunch-configuration for continuously running unit-tests in the IDE
* Health-endpoints for Kubernetes
* An outline for a CI-pipeline
* A guide for creating a HelmRelease which is used for deployment to Kubernetes
* Docs

## Tech stack

* Python 3.9
* Poetry for package management
* FastAPI for api's

## Contributing

All contributions are very much welcome by anyone! This should be a repository that evolves as teams in SSB learn what
is useful to standardise between teams, and therefore should be added to this template repository to get started with
sensible defaults. Please create a branch, propose a PR and have someone look at it before merging to main

## Prerequisites

* You should be a member of a team which is onboarded to BIP (GCP-projects and Kubernetes namespaces created)
* Some basic Python knowledge is needed

## A note on Windows operating system

* The scripts provided in the `bin`-directory are developed targeting macOS/Linux
* `gunicorn` which handles the FastAPI does not work on Windows, so the only option to run the app on your machine is
by running the Docker image. See description below

## Recommendation

It's recommended to complete the setup in this README, including deploying the app "as-is" to at least the staging
cluster. This creates a complete pipeline for CI and CD, so each merge to the main branch will result in a deployment
to Kubernetes. This will allow for the deployment configuration to evolve ass the business logic in the app evolves,
resulting in a less error-prone process

If developing the app using other frameworks than FastAPI and Gunicorn or a "plain" Python backend app,
the scripts and Dockerfile provided must be tuned to reflect this

## Usage

### Create a new repository

The following steps will create a new repository for your app, using this repository as a temlplate

* Navigate to <https://github.com/orgs/statisticsnorway/repositories> and click `New repository`.
* In the `Repository template` dropdown choose this repository as a template.
* Fill in the rest of the fields with the info for your new repo. You might omit the `Add a README file` and
`Add .gitignore` options as these files are contained in the template repository.
* Adjust the repository on GitHub to suit your needs. This should include the `Collaborators and teams` and
`Branches->Branch protection rules` configuration under `Settings`

### Setup environment

The following steps will clone the new repository and set up the environment for further development

* Clone your newly created repository
* Update the descriptive fields of `pyproject.toml` with details about your app, particularly the `name`, `description` and `authors` fields must be updated.
* In the root of your repository, run the command `make setup-dev` to install dependencies
* To run the tests included, run `poetry run pytest` from the root of the repository
* If you'd like to continuously run the tests in your IDE, the [Pycrunch](https://pycrunch.com) plugin should be
installed (works with Pycharm and IntelliJ)
* To run the webserver on your machine, run `make run-dev`. Navigate to <http://localhost:8080/health/alive>
to see if the app is alive
* To run the Docker image on your machine, run `make run-docker-dev`. Navigate to <http://localhost:8080/health/alive>
to see if the app is alive

The endpoints exposed from this simple app are:

* <http://localhost:8080/docs>
* <http://localhost:8080/metrics>
* <http://localhost:8080/health/alive>
* <http://localhost:8080/health/ready>

### Troubleshooting

#### Incorrect module discovered by pytest

If, when running `poetry run pytest` you get errors like below

```shell
====================================================================================================== ERRORS =======================================================================================================
____________________________________________________________________ ERROR collecting lib/python3.8/site-packages/sniffio/_tests/test_sniffio.py ____________________________________________________________________
import file mismatch:
imported module 'sniffio._tests.test_sniffio' has this __file__ attribute:
  /Users/mmwinther/Library/Caches/pypoetry/virtualenvs/microservice-template-test-FTpaP-8S-py3.9/lib/python3.9/site-packages/sniffio/_tests/test_sniffio.py
which is not the same as the test file we want to collect:
  /Users/mmwinther/code/microservice-template-test/lib/python3.8/site-packages/sniffio/_tests/test_sniffio.py
HINT: remove __pycache__ / .pyc files and/or use a unique basename for your test file modules
```

Then run `rm -rf lib/` to clear the `pytest` cache

#### Incompatible python version warning

If you get a warning like below

```shell
The currently activated Python version 3.8.12 is not supported by the project (^3.9).
Trying to find and use a compatible version.
Using python3.9 (3.9.13)
Creating virtualenv microservice-template-test-FTpaP-8S-py3.9 in /Users/mmwinther/Library/Caches/pypoetry/virtualenvs
```

Then the fix is to change the python interpreter version where poetry is installed. For example:

```shell
curl -sSL https://install.python-poetry.org | python3 - --uninstall
curl -sSL https://install.python-poetry.org | python3.9 -
```



### Using Black as code formatter

If you'd like to use [Black](https://pypi.org/project/black/) as a code formatter in your repository run the
command `make setup-pre-commit` to install Black and configure a pre-commit-hook.
The commit will fail if Black reformats any files, so a new commit must be initiated

### CI: Build the app

The following steps will create a pipeline in Azure Pipelines, build a Docker image and push it to Google Container
registry for later deployment:

* Create a new branch in the repo
* Run the command `make init-app` which will replace som placeholders in `pipeline/azure-pipeline.yaml` and
`deploy/helmrelease.yaml`
* Commit the changes to GitHub
* Open <https://dev.azure.com/statisticsnorway/> in a browser, find your team, click `Pipelines` and `New pipeline`
* Choose your repo, and the provided `azure-pipeline.yaml` in the new branch as source
* Run the pipeline
* Create a PR and after tests and pipeline has run OK; merge the branch to main.
* The pipeline will now trigger yet another run, and it's this image that later will be deployed
* Open `https://console.cloud.google.com/gcr/images/prod-bip/eu/ssb/<teamname>/<appname>?project=prod-bip` in a
browser. Replace `<teamname>` and `<appname>` with correct values.
* Look for a tag on the image which starts with `main-` and copy this tag. This is needed later

For more info on CI, please visit [SSB developer guide](https://docs.bip.ssb.no/develop/azure_pipelines/)

### CD: Deploy the app to Kubernetes

The following steps will deploy the app (the Docker image created in the previous step) to Kubernetes

* Open <https://start.bip.ssb.no/> in a browser, and complete the form
* Name=`appname`
* Namespace=`teamname`
* Billing project=`teamname`
* Application Type=`Backend`
* Cluster environment=`Staging`
* Container repository=`eu.gcr.io/prod-bip/ssb/<teamname>/<appname>` (replace appname and teamname with the values used
in previous steps )
* Tag pattern=`leave unchanged`
* Image tag=`the tag copied from the image in the CI steps`
* Exposed=`leave unchanged`
* Authenticated=`leave unchanged`
* Health probes=`leave unchanged`
* Collect metrics=`leave unchanged`
* Click submit

In the browser a generated HelmRelease will be displayed. Copy the the contents.

* Clone the [platform-dev](https://github.com/statisticsnorway/platform-dev/tree/master) repo, and create a new branch
* Create a new file in the repository. The file should be located under the folder
`flux/staging-bip-app/<teamname>/<appname>` and named `<appname>.yaml`. Paste the generated HelmRelease in this file
* Push the branch to GitHub and wait for the pipeline on this repo to complete
* If the pipeline runs ok, merge the branch to master

Your app should now, after some minutes, be deployed to Kubernetes. Use `kubectl port forward` to access the endpoints
on the app running in Kubernetes

For more info on deploying an app to Kubernetes, also please visit [SSB developer guide](https://docs.bip.ssb.no/deploy/)
The HelmRelease may also be created by visiting [https://start.bip.ssb.no/#](https://start.bip.ssb.no/#)

### Develop the app

Now that the repository and CI/CD pipelines are in place it's time to fill the app with the exciting business logic. All
changes that are merged to the main-branch in the app's reop will be deployed to Kubernetes

* Replace this README with one that documents your app
* The rest is up to you ;-)

## Improvements and stuff not yet implemented

* Scripts supporting Windows OS should be added
* Would adding unit tests which utilizes mocking be valuable?
* Is it possible to reach the backend for <https://start.bip.ssb.no/>? This way the HelmRelease could be generated using
and api-call
