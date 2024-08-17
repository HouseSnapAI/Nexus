import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import * as docker from "@pulumi/docker";
import * as path from "path";
import * as dotenv from "dotenv";

// Load environment variables from .env file
dotenv.config();

// Create an SQS Queue
const queue = new aws.sqs.Queue("NexusQueue", {
    visibilityTimeoutSeconds: 30,
});

// Create an IAM Role for the Lambda function
const role = new aws.iam.Role("NexusLambdaRole", {
    assumeRolePolicy: JSON.stringify({
        Version: "2012-10-17",
        Statement: [
            {
                Action: "sts:AssumeRole",
                Principal: {
                    Service: "lambda.amazonaws.com",
                },
                Effect: "Allow",
                Sid: "",
            },
        ],
    }),
});

// Attach the AWSLambdaBasicExecutionRole policy to the role
const rolePolicyAttachment = new aws.iam.RolePolicyAttachment("lambdaRoleAttachment", {
    role: role.name,
    policyArn: "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
});

// Add a policy to allow the Lambda function to read messages from the SQS queue
const queuePolicy = new aws.iam.RolePolicy("queuePolicy", {
    role: role.id,
    policy: queue.arn.apply(queueArn => JSON.stringify({
        Version: "2012-10-17",
        Statement: [
            {
                Effect: "Allow",
                Action: [
                    "sqs:ReceiveMessage",
                    "sqs:DeleteMessage",
                    "sqs:GetQueueAttributes"
                ],
                Resource: queueArn,
            },
        ],
    })),
});

// Create an ECR repository
const repo = new aws.ecr.Repository("my-lambda-repo");

// Build and publish the Docker image
const image = new docker.Image("my-lambda-image", {
    build: {
        context: path.join(__dirname, "lambda"),
    },
    imageName: pulumi.interpolate`${repo.repositoryUrl}:latest`,
    registry: {
        server: repo.repositoryUrl,
        username: aws.ecr.getAuthorizationToken().then(token => token.userName),
        password: aws.ecr.getAuthorizationToken().then(token => token.password),
    },
});

// Create the Lambda function
const lambda = new aws.lambda.Function("NexusLambda", {
    packageType: "Image",
    imageUri: image.imageName,
    role: role.arn,
    environment: {
        variables: {
            SUPABASE_URL: process.env.SUPABASE_URL!,
            SUPABASE_ANON_KEY: process.env.SUPABASE_ANON_KEY!,
        },
    },
});

// Create an Event Source Mapping to trigger the Lambda function from the SQS Queue
const eventSourceMapping = new aws.lambda.EventSourceMapping("eventSourceMapping", {
    eventSourceArn: queue.arn,
    functionName: lambda.name,
    batchSize: 10,
});

// Export the queue URL and the Lambda function name
export const queueUrl = queue.url;
export const lambdaFunctionName = lambda.name;