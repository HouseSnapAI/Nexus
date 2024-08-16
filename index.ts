import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";

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

// Create the Lambda function
const lambda = new aws.lambda.Function("NexusLambda", {
    runtime: aws.lambda.Python3d8Runtime,
    role: role.arn,
    handler: "index.handler",
    code: new pulumi.asset.AssetArchive({
        ".": new pulumi.asset.FileArchive("./lambda"),
    }),
    environment: {
        variables: {
            QUEUE_URL: queue.url,
        },
    },
});

// Create an Event Source Mapping to trigger the Lambda function from the SQS Queue
const eventSourceMapping = new aws.lambda.EventSourceMapping("eventSourceMapping", {
    eventSourceArn: queue.arn,
    functionName: lambda.name,
    batchSize: 10,
    startingPosition: "LATEST",
});

// Export the queue URL and the Lambda function name
export const queueUrl = queue.url;
export const lambdaFunctionName = lambda.name;