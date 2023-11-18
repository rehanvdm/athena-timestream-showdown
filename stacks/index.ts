import * as cdk from 'aws-cdk-lib';
import TimestreamBackend from './timestream_backend';
import AthenaBackend from './athena_backend';

const app = new cdk.App();
async function Main() {

  const env = {
    region: "eu-west-1",
    account: "12344567890",
  };
  // console.log('CDK ENV', env);

  new TimestreamBackend(app, 'showdown-timestream', { env });

  new AthenaBackend(app, 'showdown-athena', { env }, {
    awsEnv: env,
    sites: ['showdown'],
  });

  app.synth();
}

Main().catch((err) => {
  console.error(err);
  process.exit(1);
});

