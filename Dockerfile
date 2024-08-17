# Define function directory
ARG FUNCTION_DIR="/lambda"

FROM mcr.microsoft.com/playwright/python:v1.46.0-focal AS build-image

# Install aws-lambda-cpp build dependencies
RUN apt-get update && \
    apt-get install -y \
    g++ \
    make \
    cmake \
    unzip \
    libcurl4-openssl-dev

# Include global arg in this stage of the build
ARG FUNCTION_DIR
# Create function directory
RUN mkdir -p ${FUNCTION_DIR}

# Copy function code
COPY lambda/* ${FUNCTION_DIR}

# Install the runtime interface client and other dependencies
RUN pip3 install  \
    --target ${FUNCTION_DIR} \
    awslambdaric && \
    pip3 install -r ${FUNCTION_DIR}/requirements.txt --target ${FUNCTION_DIR}

# Install Playwright
RUN pip3 install playwright && \
    playwright install

# Multi-stage build: grab a fresh copy of the base image
FROM mcr.microsoft.com/playwright/python:v1.46.0-focal

# Include global arg in this stage of the build
ARG FUNCTION_DIR
# Set working directory to function root directory
WORKDIR ${FUNCTION_DIR}

# Copy in the build image dependencies
COPY --from=build-image ${FUNCTION_DIR} ${FUNCTION_DIR}

ENV PYTHONPATH=${FUNCTION_DIR}

# Download the AWS Lambda Runtime Interface Emulator (RIE) if running locally
ADD https://github.com/aws/aws-lambda-runtime-interface-emulator/releases/latest/download/aws-lambda-rie /usr/local/bin/aws-lambda-rie
RUN chmod +x /usr/local/bin/aws-lambda-rie

# Use the RIE to run the Lambda function if running locally
ENTRYPOINT ["/usr/local/bin/aws-lambda-rie", "python3", "-m", "awslambdaric", "index.handler"]

# Use the AWS Lambda runtime interface client to start the function
CMD ["python3", "-m", "awslambdaric", "index.handler"]