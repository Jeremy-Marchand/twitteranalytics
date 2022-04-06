# project id
PROJECT_ID=wagon-bootcamp-802

# Docker image name
DOCKER_IMAGE_NAME=image-test



##### LOCAL API RUN - - - - - - -
run_api:
	uvicorn api.fast:app --reload  # load web server with code autoreload

##### DOCKER - - - - - - - - - - -

docker_build:
	docker build -t eu.gcr.io/${PROJECT_ID}/${DOCKER_IMAGE_NAME} .

docker_run_local:
	docker run -e PORT=8000 -p 8000:8000 eu.gcr.io/${PROJECT_ID}/${DOCKER_IMAGE_NAME}

docker_push:
	docker push eu.gcr.io/${PROJECT_ID}/${DOCKER_IMAGE_NAME}

gcloud_run:
	gcloud run deploy --image eu.gcr.io/${PROJECT_ID}/${DOCKER_IMAGE_NAME} \
		--platform managed \
		--region europe-west1
