FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

# Copy everything
COPY . .

# Install core requirements using uv
WORKDIR /core
RUN uv pip install --system -r requirements.txt

WORKDIR /
# Run the main application
CMD ["python", "/package_managers/pkgx/main.py"] 
