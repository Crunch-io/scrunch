FROM python:3.11-slim

WORKDIR /app

# Install git (needed for pycrunch install from GitHub)
RUN apt-get update && apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

# Install tox
RUN pip install --upgrade pip && pip install tox tox-gh-actions

# Copy the project
COPY . .

# Default: run tox for py311
CMD ["tox", "-e", "py311"]
