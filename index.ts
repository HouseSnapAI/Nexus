import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import * as path from "path";
import * as dotenv from "dotenv";

// Load environment variables from .env file
dotenv.config();

// Create an SQS Queue
const queue = new aws.sqs.Queue("NexusQueue", {
    visibilityTimeoutSeconds: 300, // Set visibility timeout to match Lambda function timeout
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

// Attach the AmazonEC2ContainerRegistryReadOnly policy to the role
const ecrReadOnlyPolicyAttachment = new aws.iam.RolePolicyAttachment("ecrReadOnlyPolicyAttachment", {
    role: role.name,
    policyArn: "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
});

// Attach the AmazonEC2ContainerRegistryPowerUser policy to the role
const ecrPowerUserPolicyAttachment = new aws.iam.RolePolicyAttachment("ecrPowerUserPolicyAttachment", {
    role: role.name,
    policyArn: "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryPowerUser",
});

// Create a more comprehensive ECR access policy
const ecrAccessPolicy = new aws.iam.Policy("ecrAccessPolicy", {
    description: "Policy for Lambda to access ECR",
    policy: JSON.stringify({
        Version: "2012-10-17",
        Statement: [
            {
                Effect: "Allow",
                Action: [
                    "ecr:GetAuthorizationToken",
                    "ecr:BatchCheckLayerAvailability",
                    "ecr:GetDownloadUrlForLayer",
                    "ecr:BatchGetImage",
                    "ecr:InitiateLayerUpload",
                    "ecr:UploadLayerPart",
                    "ecr:CompleteLayerUpload",
                    "ecr:PutImage"
                ],
                Resource: "*"
            }
        ]
    })
});

// Attach the ECR access policy to the Lambda execution role
const ecrPolicyAttachment = new aws.iam.RolePolicyAttachment("ecrPolicyAttachment", {
    role: role.name,
    policyArn: ecrAccessPolicy.arn,
});

// Create a policy for SQS access
const sqsAccessPolicy = new aws.iam.Policy("sqsAccessPolicy", {
    description: "Policy for Lambda to access SQS",
    policy: pulumi.output(queue.arn).apply(arn => JSON.stringify({
        Version: "2012-10-17",
        Statement: [
            {
                Effect: "Allow",
                Action: [
                    "sqs:ReceiveMessage",
                    "sqs:DeleteMessage",
                    "sqs:GetQueueAttributes"
                ],
                Resource: arn
            }
        ]
    }))
});

// Attach the SQS access policy to the Lambda execution role
const sqsPolicyAttachment = new aws.iam.RolePolicyAttachment("sqsPolicyAttachment", {
    role: role.name,
    policyArn: sqsAccessPolicy.arn,
});

// Create the Lambda function
const lambda = new aws.lambda.Function("NexusLambda", {
    packageType: "Image",
    imageUri: "767397951738.dkr.ecr.us-west-1.amazonaws.com/nexus-engine:latest",
    role: role.arn,
    timeout: 300, // Set timeout to 5 minutes
    memorySize: 2048, 
    environment: {
        variables: {
            SUPABASE_URL: process.env.SUPABASE_URL!,
            SUPABASE_ANON_KEY: process.env.SUPABASE_ANON_KEY!,
        },
    },
}, {
    dependsOn: [ecrPolicyAttachment, sqsPolicyAttachment], // Ensure the policies are attached before creating the Lambda
});

// Create an Event Source Mapping to trigger the Lambda function from the SQS Queue
const eventSourceMapping = new aws.lambda.EventSourceMapping("eventSourceMapping", {
    eventSourceArn: queue.arn,
    functionName: lambda.name,
    batchSize: 1,
});