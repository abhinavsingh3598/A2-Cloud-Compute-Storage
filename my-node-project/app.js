

const express = require('express');
const bodyParser = require('body-parser');
const grpc = require('grpc');
const protoLoader = require('@grpc/proto-loader');

const app = express();

app.use(bodyParser.json());

// Load protobuf
const protoPath = 'computeandstorage.proto';
const packageDefinition = protoLoader.loadSync(protoPath, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});
const computeAndStorageProto = grpc.loadPackageDefinition(packageDefinition).computeandstorage;

// Create gRPC client
const client = new computeAndStorageProto.EC2Operations('localhost:50051', grpc.credentials.createInsecure());

app.post('/storedata', async (req, res) => {
  const { data } = req.body;

  client.StoreData({ data }, (err, response) => {
    if (err) {
      console.error(err);
      res.status(500).send('Internal server error');
    } else {
      res.status(200).json({ s3uri: response.s3uri });
    }
  });
});

app.patch('/appenddata', (req, res) => {
  const { data } = req.body;

  client.AppendData({ data }, (err, response) => {
    if (err) {
      console.error(err);
      res.status(500).send('Internal server error');
    } else {
      res.status(200).json({ s3uri: response.s3uri });
    }
  });
});

app.delete('/deletefile', (req, res) => {
  const { s3uri } = req.body;

  client.DeleteFile({ s3uri }, (err, response) => {
    if (err) {
      console.error(err);
      res.status(500).send('Internal server error');
    } else {
      res.status(200).json({ s3uri: response.s3uri });
    }
  });
});

// // app.post('/start', (req, res) => {
// //   const { banner, ip } = req.body;
// //   // You might want to validate the banner ID and IP here

// //   res.status(200).send('OK');
// // });

app.listen(3000, () => {
  console.log('App is listening on port 3000');
});
