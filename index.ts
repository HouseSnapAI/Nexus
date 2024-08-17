import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import * as fs from "fs";
import * as path from "path";
import * as archiver from "archiver";
import * as dotenv from "dotenv";
import { execSync } from "child_process";

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

// Create a zip file with the Lambda function and its dependencies
const lambdaDir = path.join(__dirname, "lambda");
const zipFilePath = path.join(__dirname, "lambda.zip");

// Install dependencies for the correct architecture
execSync(`pip install -r ${path.join(lambdaDir, "requirements.txt")} --platform manylinux2014_x86_64 --target ${lambdaDir} --only-binary=:all:`);

const output = fs.createWriteStream(zipFilePath);
const archive = archiver("zip", {
    zlib: { level: 9 }
});

output.on("close", function () {
    console.log(archive.pointer() + " total bytes");
    console.log("archiver has been finalized and the output file descriptor has closed.");
});

archive.on("error", function (err:any) {
    throw err;
});

archive.pipe(output);

// Add Lambda function code
archive.directory(lambdaDir, false);

// Add dependencies
archive.file(path.join(lambdaDir, "requirements.txt"), { name: "requirements.txt" });
archive.finalize();

// Create the Lambda function
const lambda = new aws.lambda.Function("NexusLambda", {
    runtime: aws.lambda.Runtime.Python3d11,
    role: role.arn,
    handler: "index.handler",
    code: new pulumi.asset.FileArchive(zipFilePath),
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