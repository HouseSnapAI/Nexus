# Define function directory
ARG FUNCTION_DIR="/lambda"

FROM python:3.11-bookworm

# Include global arg in this stage of the build
# removthis is shi dont work
ARG FUNCTION_DIR 

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PLAYWRIGHT_BROWSERS_PATH=${FUNCTION_DIR}/ms-playwright

# Install aws-lambda-cpp build dependencies
RUN apt-get update && \
    apt-get install -y \
    g++ \
    make \
    cmake \
    unzip \
    libcurl4-openssl-dev \
    gconf-service \
    libasound2 \
    libatk1.0-0 \
    libcairo2 \
    libcups2 \
    libfontconfig1 \
    libgdk-pixbuf2.0-0 \
    libgtk-3-0 \
    libnspr4 \
    libpango-1.0-0 \
    libxss1 \
    fonts-liberation \
    libappindicator1 \
    libnss3 \
    lsb-release \
    xdg-utils && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create function directory
RUN mkdir -p ${FUNCTION_DIR}

# Copy function code
COPY lambda/* ${FUNCTION_DIR}

# Install the runtime interface client and other dependencies
RUN pip install  \
    --target ${FUNCTION_DIR} \
    awslambdaric && \
    pip install -r ${FUNCTION_DIR}/requirements.txt --target ${FUNCTION_DIR}

# Change to FUNCTION_DIR, install Playwright and browsers, then change back
RUN cd ${FUNCTION_DIR} && \
    pip install playwright==1.46.0 && \
    PLAYWRIGHT_BROWSERS_PATH=${FUNCTION_DIR}/ms-playwright python -m playwright install --with-deps chromium && \
    cd -

# Set working directory to function root directory
WORKDIR ${FUNCTION_DIR}

ENV PYTHONPATH=${FUNCTION_DIR}

# Download the AWS Lambda Runtime Interface Emulator (RIE) if running locally
#ADD https://github.com/aws/aws-lambda-runtime-interface-emulator/releases/latest/download/aws-lambda-rie /usr/local/bin/aws-lambda-rie
#RUN chmod +x /usr/local/bin/aws-lambda-rie

# Use the RIE to run the Lambda function if running locally
#ENTRYPOINT ["/usr/local/bin/aws-lambda-rie", "python3", "-m", "awslambdaric"]
# ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]

# Use the AWS Lambda runtime interface client to start the function
CMD ["python3", "-m", "awslambdaric", "index.handler"]
#CMD [ "index.handler" ]

# use aws lambda base image 