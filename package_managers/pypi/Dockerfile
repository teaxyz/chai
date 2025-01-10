FROM python:3.11-slim

WORKDIR /app

COPY package_managers/pypi/requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "-m", "package_managers.pypi.main"]
