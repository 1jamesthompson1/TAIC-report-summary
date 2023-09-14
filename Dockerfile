# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at
COPY viewer /app/viewer
COPY engine /app/engine
COPY config.yaml /app
COPY pyproject.toml /app
COPY poetry.lock /app


# Install Poetry
RUN pip install poetry

# Install the project dependencies using Poetry
RUN poetry install --no-root --only viewer

# Expose the port your application will run on
EXPOSE 5000

# Run the application
CMD ["poetry", "run", "python", "viewer/app.py", "--host=0.0.0.0"]
