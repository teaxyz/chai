FROM python:3.11

# Copy everything
COPY . .

# Install core requirements
WORKDIR /core
RUN pip install --no-cache-dir -r requirements.txt

# Install pkgx requirements
WORKDIR /package_managers/pkgx
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /
# Run the main application
CMD ["python", "/package_managers/pkgx/main.py"] 
