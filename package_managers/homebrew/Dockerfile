FROM ubuntu:24.10
RUN apt-get update && \
    apt-get install -y jq curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
COPY . .
WORKDIR /package_managers/homebrew
RUN chmod +x /package_managers/homebrew/pipeline.sh
CMD ["/package_managers/homebrew/pipeline.sh"]

