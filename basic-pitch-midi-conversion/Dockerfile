#ChatGPT helped me make this

# Define custom function directory
ARG FUNCTION_DIR="/function"

# Use an Ubuntu base image
FROM ubuntu:latest

# Include global arg in this stage of the build
ARG FUNCTION_DIR

# Create directories for cache and configuration
RUN mkdir -m 777 /tmp/NUMBA_CACHE_DIR /tmp/MPLCONFIGDIR || true
ENV NUMBA_CACHE_DIR=/tmp/NUMBA_CACHE_DIR/
ENV MPLCONFIGDIR=/tmp/MPLCONFIGDIR/

# Install Python and necessary dependencies
RUN apt-get update && \
    apt-get install -y python3.10 python3-pip wget build-essential

# Upgrade pip, setuptools, and wheel
RUN python3 -m pip install --upgrade pip setuptools wheel

# Install aws-lambda-ric for Lambda Runtime Interface Client
RUN python3 -m pip install awslambdaric

# Create the function directory
RUN mkdir -p ${FUNCTION_DIR}

# Copy the function code and requirements.txt to the function directory
COPY ./app/* ${FUNCTION_DIR}
COPY requirements.txt ${FUNCTION_DIR}

# Set the working directory to the function directory
WORKDIR ${FUNCTION_DIR}

# Install the dependencies
RUN python3 -m pip install numpy==1.24.4
RUN python3 -m pip install -r requirements.txt --target ${FUNCTION_DIR}

# Set the CMD to your handler
ENTRYPOINT [ "python3", "-m", "awslambdaric" ]

CMD ["lambda_function.lambda_handler"]
