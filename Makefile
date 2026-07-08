validate:
	docker run --rm -v ${PWD}:/app asyncapi/cli:latest \
	validate /app/asyncapi.yaml

docs:
	docker run --rm \
		-v ${PWD}:/app \
		asyncapi/cli:latest \
		generate fromTemplate \
		/app/asyncapi.yaml \
		@asyncapi/html-template \
		--output /app/docs



