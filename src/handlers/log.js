import {
  S3Client,
  GetObjectCommand
} from "@aws-sdk/client-s3";

import {
  DynamoDBClient,
  PutItemCommand
} from "@aws-sdk/client-dynamodb";

import { Readable } from "stream";

const s3 = new S3Client({ region: process.env.REGION });
const dynamo = new DynamoDBClient({ region: process.env.REGION });

const TABLE = process.env.LOG_TRACE_DB;
const BUCKET = process.env.LOG_TRACE_BUCKET;

// Convert S3 Body stream to JSON
const streamToString = async (stream) =>
  await new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (chunk) => chunks.push(chunk));
    stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf-8")));
    stream.on("error", reject);
  });

export const handler = async (event) => {
  try {
    for (const record of event.Records) {
      const body = JSON.parse(record.body);
      console.log("body********", body);

      const { key } = body;

      const s3Resp = await s3.send(
        new GetObjectCommand({
          Bucket: BUCKET,
          Key: key,
        })
      );

      const fileContent = await streamToString(s3Resp.Body);
      const json = JSON.parse(fileContent);

      console.log("S3 JSON:", json);

      // 2️⃣ Insert into DynamoDB
      const item = {
        id: { S: json.id },
        userId: { S: json.userId },
        userType: { S: json.userType },
        stepCode: { N: String(json.stepCode) },
        status: { S: json.status },
        request: { S: JSON.stringify(json.request) },
        response: { S: JSON.stringify(json.response) || 'null' },
        searchKey: { S: json.searchKey },
        createdAt: { S: new Date().toISOString() },
      };

      // ✅ Add offerId only if it exists
      if (json?.offerId) {
        item.offerId = { S: json.offerId };
      }

      if (json?.hotelKey) {
        item.hotelKey = { S: json.hotelKey };
      }

      const putCmd = new PutItemCommand({
        TableName: TABLE,
        Item: item,
      });

      await dynamo.send(putCmd);
      console.log("DynamoDB write successs");
    }
  } catch (err) {
    console.error("log tracer error", err);
  }
};
