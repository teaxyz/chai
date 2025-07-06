FROM python:3.11

# Copy everything from the root directory (build context)
COPY . .

# Install core requirements
WORKDIR /core
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /

# Run the main application
CMD ["python", "/package_managers/crates/main.py"]
