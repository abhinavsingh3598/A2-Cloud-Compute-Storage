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
    accessKeyId: 'ASIAQTFKHOFCWUIPKWBG',
    secretAccessKey: 'RnZsE0dHXGFv6RymM6HmLhTcmVi5rzO2tugTdPV1',
    sessionToken: 'FwoGZXIvYXdzEOj//////////wEaDLjMdu33OfeVpNl4HyK+AaoPGCfzApm+fABmPMxdZdAHVVRVGKCbVjhYZpVWS4UpzSwmYGAdLClntKrT42sCcgZwFH9/yYMIZfo2cq8Dkaj3If4haL/u0vTIv65tNJF0D/UIrdZU46HNyZtYzKy+RGNxbprkFIMS4ASPXJVR1X2FL8EETbYBf8eghZSKNJXT5EHogjtw/jVstY/cii8qjvDihLiK/8FqqoyPx7lEfSLCYAfUBFL57M3IRjcV/xDSHSt051sYsZ41RKQrLb8okZ30owYyLRh86t41inHkp6po0pqIQpYBG1F9Y7L0rpnIwIm9que6y4bAqOUfDa1arNuzwQ==',
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
