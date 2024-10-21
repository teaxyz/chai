FROM python:3.11
RUN apt-get update && \
    apt-get install -y jq curl postgresql-client && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
COPY . .
WORKDIR /package_managers/homebrew
RUN chmod +x /package_managers/homebrew/pipeline.sh
# RUN pip install --no-cache-dir -r requirements.txt
CMD ["/package_managers/homebrew/pipeline.sh"]

