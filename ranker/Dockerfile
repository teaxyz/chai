FROM python:3.11

# Copy everything
COPY . . 

# Install core requirements 
WORKDIR /core 
RUN pip install --no-cache-dir -r requirements.txt 

# Install ranker requirements 
WORKDIR /ranker 
RUN pip install --no-cache-dir -r requirements.txt 

# Command to run the application
CMD ["python", "/ranker/main.py"] 