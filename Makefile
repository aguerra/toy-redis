run:
	@poetry run python -m toy_redis.server

test:
	@poetry run pytest

mypy:
	@poetry run mypy .
