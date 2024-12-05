# Use the official Python image from the Docker Hub
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file to the working directory
COPY required_library.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r required_library.txt

# Copy the rest of the application code to the working directory
COPY . .

# Expose the port that the app runs on
EXPOSE 1007

# Set the environment variable to disable output buffering 
ENV PYTHONUNBUFFERED=1

# Run the application
CMD ["python", "Ligado-HelloWorld.py"]
