FROM python:3.13.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code

# Add virtual environment to PATH so we can use installed packages
ENV PATH="/code/.venv/bin:$PATH"

COPY pyproject.toml .python-version uv.lock ./

RUN uv sync --locked

COPY ./module_3/load_yellow_taxi_data.py .

ENTRYPOINT ["bash"]