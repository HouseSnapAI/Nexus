# Define function directory
ARG FUNCTION_DIR="/lambda"

FROM python:3.11-bookworm

# Install aws-lambda-cpp build dependencies
RUN apt-get update && \
    apt-get install -y \
    g++ \
    make \
    cmake \
    unzip \
    libcurl4-openssl-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Include global arg in this stage of the build
ARG FUNCTION_DIR
# Create function directory
RUN mkdir -p ${FUNCTION_DIR}

# Copy function code
COPY lambda/* ${FUNCTION_DIR}

# Install the runtime interface client and other dependencies
RUN pip install  \
    --target ${FUNCTION_DIR} \
    awslambdaric && \
    pip install -r ${FUNCTION_DIR}/requirements.txt --target ${FUNCTION_DIR}

RUN pip install playwright==1.46.0 && \
    playwright install --with-deps

# Set working directory to function root directory
WORKDIR ${FUNCTION_DIR}

ENV PYTHONPATH=${FUNCTION_DIR}

# Download the AWS Lambda Runtime Interface Emulator (RIE) if running locally
ADD https://github.com/aws/aws-lambda-runtime-interface-emulator/releases/latest/download/aws-lambda-rie /usr/local/bin/aws-lambda-rie
RUN chmod +x /usr/local/bin/aws-lambda-rie

# Use the RIE to run the Lambda function if running locally
# ENTRYPOINT ["/usr/local/bin/aws-lambda-rie", "python3", "-m", "awslambdaric"]
# ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]

# Use the AWS Lambda runtime interface client to start the function
CMD ["index.handler"]