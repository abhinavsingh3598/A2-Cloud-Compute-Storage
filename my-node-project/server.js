const grpc = require('grpc');
const protoLoader = require('@grpc/proto-loader');
const AWS = require('aws-sdk');

const packageDefinition = protoLoader.loadSync('computeandstorage.proto', {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});
const computeAndStorageProto = grpc.loadPackageDefinition(packageDefinition).computeandstorage;

// Configure the AWS SDK
AWS.config.update({
    accessKeyId: 'ASIAQTFKHOFC32M2RYKW',
    secretAccessKey: 'wKtrw6TixHyLXa9UBYFpK3A+uBnQeoG64VmU6vcU',
    sessionToken: 'FwoGZXIvYXdzEEcaDE/6AK5JQBGg578sXSK+Abar2+48P2ORq47JE7/ut/ENSeZUgCC8m+IBEG7Qex+l7Vv7MzxHYYcCxyDA1m8VK/gMK/OKJbtM0yQ80lyLt2BAUYAU7ocF+yaz8nuEbCkOHLYfFkMjxeBw5Q3QEnqi+EQkNRYxD3TrBmpiJOBHot51iXoordDYgXceOIsgfHFr9fT0e0Tt5lWs/5fM0HsxzEWVv5NJwN0rIxVBwgRgZocsN4JSCQffAZFGbp1bo9ibcFXlr8UCT1tmPv8iZ/go7pCJpAYyLZnQzio7X5p/VC10FQzI1qfS8n4eo4IYXiRk8UuK+G4EPnvaTF7xNW87bgqs5w==',
    region: 'us-east-1',
  });
const s3 = new AWS.S3({ apiVersion: '2006-03-01' });

//-------------- store data ----------------------
async function storeData(call, callback) {
  const { data } = call.request;

  const params = {
    Bucket: 'cloud-bucket-5409',
    Key: `file.txt`,
    Body: data,
  };

  try {
    const result = await s3.upload(params).promise();
    callback(null, { s3uri: result.Location });
  } catch (err) {
    console.error(err);
    callback(err);
  }
}
  
  // ------------------------append data---------------
  async function appendData(call, callback) {
    const { data } = call.request;
  
    const params = {
      Bucket: 'cloud-bucket-5409', 
      Key: `file.txt`,
    };
  
    try {
      // Download the existing file
      const originalFile = await s3.getObject(params).promise();
      const originalData = originalFile.Body.toString('utf-8');
  
      // Append new data
      const updatedData = originalData + data;
      params.Body = updatedData;
  
      // Re-upload the file
      const result = await s3.upload(params).promise();
  
      // Verify data
      // const uploadedFile = await s3.getObject(params).promise();
      // const uploadedData = uploadedFile.Body.toString('utf-8');
  
      callback(null, { s3uri: result.Location });
    
    } catch (err) {
      console.error(err);
      callback(err);
    }
  }
  
   // ------------------------delete data--------------- 

  async function deleteFile(call, callback) {
    const { s3uri } = call.request;
  
    const url = require('url');
    const parsedUrl = url.parse(s3uri);
    const key = decodeURIComponent(parsedUrl.pathname.slice(1));
  
    const params = {
      Bucket: 'cloud-bucket-5409',
      Key: `file.txt`,
    };
  
    try {
      await s3.deleteObject(params).promise();
      callback(null, {});
    } catch (err) {
      console.error(err);
      callback(err);
    }
  }
  

const server = new grpc.Server();
server.addService(computeAndStorageProto.EC2Operations.service, {
  StoreData: storeData,
  AppendData: appendData,
  DeleteFile: deleteFile,
});

server.bind('0.0.0.0:50051', grpc.ServerCredentials.createInsecure());
console.log('Server running at http://0.0.0.0:50051');
server.start();
