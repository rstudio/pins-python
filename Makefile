SPHINX_BUILDARGS=
RSC_API_KEYS=pins/tests/rsconnect_api_keys.json

dev: pins/tests/rsconnect_api_keys.json

dev-start:
	docker-compose up -d
	docker-compose exec -T rsconnect bash < script/setup-rsconnect/add-users.sh
	# curl fails with error 52 without a short sleep....
	sleep 5
	curl -s --retry 10 --retry-connrefused http://localhost:3939

dev-stop:
	docker-compose down
	rm -f $(RSC_API_KEYS)

$(RSC_API_KEYS): dev-start
	python script/setup-rsconnect/dump_api_keys.py $@

test:
	pytest

docs-build:
	cd docs && sphinx-build . ./_build/html $(SPHINX_BUILDARGS)

docs-watch:
	cd docs && sphinx-autobuild . ./_build/html $(SPHINX_BUILDARGS)
