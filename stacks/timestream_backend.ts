import * as cdk from 'aws-cdk-lib';
import * as timestream from 'aws-cdk-lib/aws-timestream';
import { Construct } from 'constructs';


export class TimestreamBackend extends cdk.Stack {
  constructor(scope: Construct, id: string, stackProps: cdk.StackProps) {
    super(scope, id, stackProps);

    function name(name: string): string {
      return id + '-' + name;
    }

    const dbName = name('ts-db');
    const db = new timestream.CfnDatabase(this, dbName, {
      databaseName: dbName,
    });

    const tableName = name('ts-table');
    const table = new timestream.CfnTable(this, tableName, {
      databaseName: dbName,
      tableName: tableName,
      retentionProperties: {
        memoryStoreRetentionPeriodInHours: 3, // Ingestion can take a while
        magneticStoreRetentionPeriodInDays: 30 * 12, // Keep 1 year's of data
      },
    });
    table.addDependency(db);

    new cdk.CfnOutput(this, name('TIMESTREAM_DATABASE_NAME'), {
      description: 'TIMESTREAM_DATABASE_NAME',
      value: dbName,
    });
    new cdk.CfnOutput(this, name('TIMESTREAM_TABLE_NAME'), {
      description: 'TIMESTREAM_TABLE_NAME',
      value: tableName,
    });
  }
}

export default TimestreamBackend;
