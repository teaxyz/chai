FROM python:3.11

# Copy everything
COPY . .

# Install core requirements
WORKDIR /core
RUN pip install --no-cache-dir -r requirements.txt

# Install crates requirements
WORKDIR /package_managers/crates
RUN pip install --no-cache-dir -r requirements.txt

# Run the main application
CMD ["python", "/package_managers/crates/main.py"]