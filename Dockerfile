FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
COPY scripts ./scripts
COPY migrations ./migrations
COPY alembic.ini ./
RUN pip install --no-cache-dir '.[github]'
USER 65532:65532
EXPOSE 8080
CMD ["uvicorn", "continuity.api:app", "--host", "0.0.0.0", "--port", "8080"]
