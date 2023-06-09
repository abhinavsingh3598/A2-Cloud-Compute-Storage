const grpc = require('@grpc/grpc-js');
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
    accessKeyId: 'ASIAQTFKHOFC34MNZC4W',
    secretAccessKey: 'zzIqmPdFUU/U+1umcSAyku0/TaKpzML2UiPCsj83',
    sessionToken: 'FwoGZXIvYXdzEFYaDFub6y/eN7LaEgDTRiK+AZRFVoWFSbBNvtA60YKsDKc8MmtoAuqI0mhtvKGNY4L20ZBSCuCHmtQO6vzj8W9E4gW2dHkF4VzBHi/A5048LA5G20PVMv2JvbMgH/RGEx9aef9m/HRirzbxXy1LbVfbbUOzX7GWbzk7r6jRClOKYAZH1WInXo5JUoL6fCf0tqn5EyNbJuqLtqn8P4Or7vn6ZuGOKUI7Ii1EY23+1rqkKUHZExXKthPtFJ9uXRjbAiD5MlWSyKzv/1QuiWOqfrIo+LKMpAYyLVouUjFjYd/bxW0vtKJJU3liONUeurH4T382J2Z2IJPUUuL1qTBwIGrNuq5poA==',
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

server.bindAsync('0.0.0.0:8080', grpc.ServerCredentials.createInsecure(), (err, port) => {
  if (err) {
    console.error(err);
    return;
  }
  console.log(`Server running at http://0.0.0.0:${port}`);
  server.start();
});

