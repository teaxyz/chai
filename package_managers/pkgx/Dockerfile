FROM python:3.11
COPY . .
WORKDIR /package_managers/pkgx
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "/package_managers/pkgx/main.py"] 