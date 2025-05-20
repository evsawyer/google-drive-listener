FROM python:3.12-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Copy only the requirements file first
COPY pyproject.toml .

RUN uv sync --locked

# Copy only the necessary application files
COPY main.py .
COPY drive_state.py .
COPY llama_parse_google_drive_reader.py .
COPY run_pipeline.py .

# Expose the port
EXPOSE 8080

# Run the application
CMD ["uv", "run", "python", "main.py"]