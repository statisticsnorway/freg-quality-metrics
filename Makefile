.PHONY: default
default: | help

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'


.PHONY: run-dev
run-dev: ## Run the app using gunicorn on your machine
	./bin/run-dev.sh

.PHONY: run-docker-dev
.ONESHELL:
.SILENT:
run-docker-dev: ## Creates docker image from Dockerfile and runs the image
	read -p "Enter the appname:" appname; \
	./bin/run-docker-dev.sh $$appname

.PHONY: setup-dev
setup-dev: ## Installs required packages for the app and tests
	./bin/setup-dev.sh

.PHONY: setup-pre-commit
setup-pre-commit: ## Installs Black and creates a pre-commit-hook to enforce code formatting
	./bin/setup-pre-commit.sh

.PHONY: init-app
.ONESHELL:
.SILENT:
init-app: ## Prepares the pipeline and deployment descriptor for usage
	read -p "Enter the appname:" appname; \
	read -p "Enter the teamname:" teamname; \
	./bin/init-app.sh $$appname $$teamname
