import {getData, Page} from "lib/get_data"
import {FirehoseClient, PutRecordBatchCommand, PutRecordCommand} from "@aws-sdk/client-firehose";
import { TimestreamWriteClient, WriteRecordsCommand, _Record, Dimension } from '@aws-sdk/client-timestream-write';
import { fromIni } from "@aws-sdk/credential-providers";
import {AthenaClient} from "@aws-sdk/client-athena";
import { TimestreamQueryClient, QueryCommand } from "@aws-sdk/client-timestream-query";
import {AthenaBase} from "./lib/athena_base";

const config = {
  AWS_PROFILE: "systanics-prod-exported",
  AWS_REGION: "eu-west-1",
  ATHENA_FIREHOSE_NAME: "showdown-athena-analytic-page-views-firehose",
  ATHENA_GLUE_DB_NAME: "showdown-athena-db",
  ATHENA_S3_BUCKET_NAME: "showdown-athena-bucket-analytics",
  TIMESTREAM_DATABASE_NAME: "showdown-timestream-ts-db",
  TIMESTREAM_TABLE_NAME: "showdown-timestream-ts-table",
} as const;

const awsConfig = { region: config.AWS_REGION, credentials: fromIni({profile: config.AWS_PROFILE}) };
const firehoseClient = new FirehoseClient(awsConfig);
const timestreamClientWrite = new TimestreamWriteClient(awsConfig);
const timestreamClientQuery = new TimestreamQueryClient(awsConfig);
const athenaClient = new AthenaClient(awsConfig);
const log = (data: any) => console.log( "[" + new Date().toTimeString().slice(0,8) + "] " + data);

async function loadData()
{
  async function ingest(pageViews: Page[])
  {
    const timestreamRecords: _Record[] = [];
    for (const pageView of pageViews) {
      const pageOpenedAt = new Date(pageView.page_opened_at);
      const pageViewItems = Object.entries(pageView);

      const dimensions: Dimension[] = [];
      for (const [key, value] of pageViewItems) {
        if (['time_on_page', 'page_opened_at'].includes(key)) continue;

        if(value !== undefined)
          dimensions.push({
            Name: key,
            Value: value.toString(),
            DimensionValueType: 'VARCHAR',
          });
      }

      timestreamRecords.push({
        Dimensions: dimensions,
        MeasureName: 'time_on_page',
        MeasureValue: pageView.time_on_page.toString(),
        MeasureValueType: 'DOUBLE',
        Time: pageOpenedAt.getTime().toString(),
        Version: pageView.time_on_page + 1, // offset by one because the time on page can be 0
      });
    }

    const resp = await Promise.all([
      firehoseClient.send(new PutRecordBatchCommand({
        DeliveryStreamName: config.ATHENA_FIREHOSE_NAME,
        Records: pageViews.map((pageView) => ({
          Data: Buffer.from(JSON.stringify(pageView))
        }))
      })),
      timestreamClientWrite.send(
        new WriteRecordsCommand({
          DatabaseName: config.TIMESTREAM_DATABASE_NAME,
          TableName: config.TIMESTREAM_TABLE_NAME,
          Records: timestreamRecords,
        })
      )
    ]);

    if(resp[0].FailedPutCount)
      log("Firehose failed   : " + resp[0].FailedPutCount);
    if(resp[1].RecordsIngested && resp[1].RecordsIngested.Total != pageViews.length)
      log("Timestream failed : " + resp[0].FailedPutCount);
  }

  const maxRows = 100_000;

  log("Loading data");
  let ingestedCount = 0;
  for await (const data of getData(maxRows)) // 60*60*5
  {
    await ingest(data);
    ingestedCount += data.length;
    log("Ingested " + ingestedCount + " rows");
  }
}

async function measureQueryPerformance()
{
  async function query(athenaQuery: string, timestreamQuery: string)
  {
    const athena = new AthenaBase(athenaClient, config.ATHENA_GLUE_DB_NAME,
      's3://' + config.ATHENA_GLUE_DB_NAME + '/athena-results');
    const athenaResp = await athena.query(athenaQuery);
    // const athenaResp = {queryTime: 0, queryAndResultRetrievalTime: 0};

    const startTime = Date.now();
    let tsQueryTime = 0;
    const timestreamResp = await timestreamClientQuery.send(
      new QueryCommand({
        QueryString: timestreamQuery,
        // MaxRows: 2000,
      })
    );
    tsQueryTime = Date.now() - startTime;

    return {athenaResp, tsResponse: { ...timestreamResp, tsQueryTime }};
  }

  function calculateMetrics(times: number[])
  {
    const min = Math.min(...times);
    const max = Math.max(...times);
    const avg = times.reduce((a, b) => a + b) / times.length;
    const stdDev = Math.sqrt(times.map(x => Math.pow(x - avg, 2)).reduce((a, b) => a + b) / times.length);
    return {min, max, avg: Math.round(avg), stdDev: Math.round(stdDev), requests: times.join(', ')}
  }

  const hoursBehind = 3;
  const page_opened_at_date = (new Date()).toISOString().slice(0, 10);
  const tests = [
    {
      name: "Count all",
      athenaQuery: `
        SELECT COUNT(*) as "count" FROM page_views
        WHERE site = 'showdown' AND page_opened_at_date = '${page_opened_at_date}'
              AND page_opened_at BETWEEN now() - interval '${hoursBehind}' hour AND now()
      `,
      timestreamQuery: `
        SELECT COUNT(*) as "count" FROM "${config.TIMESTREAM_DATABASE_NAME}"."${config.TIMESTREAM_TABLE_NAME}"
        WHERE time between ago(${hoursBehind}h) and now() and site = 'showdown'
      `,
    },
    {
      name: "Count page_view",
      athenaQuery: `
          SELECT COUNT(page_id) as "count" FROM page_views
        WHERE site = 'showdown' AND page_opened_at_date = '${page_opened_at_date}'
              AND page_opened_at BETWEEN now() - interval '${hoursBehind}' hour AND now()
      `,
      timestreamQuery: `
          SELECT COUNT(page_id) as "count" FROM "${config.TIMESTREAM_DATABASE_NAME}"."${config.TIMESTREAM_TABLE_NAME}"
          WHERE time between ago(${hoursBehind}h) and now() and site = 'showdown'
      `,
    },
    {
      name: "Page views & stats",
      athenaQuery: `
        SELECT
          site,
          page_url,
          COUNT(*) as "views",
          ROUND(AVG(time_on_page),2) as "avg_time_on_page"
        FROM page_views
        WHERE site = 'showdown' AND page_opened_at_date = '${page_opened_at_date}'
              AND page_opened_at BETWEEN now() - interval '${hoursBehind}' hour AND now()
        GROUP BY site, page_url
        ORDER BY views DESC, page_url ASC
        LIMIT 1000
      `,
      timestreamQuery: `
        SELECT
          site,
          page_url,
          COUNT(*) as "views",
          ROUND(AVG(measure_value::double),2) as "avg_time_on_page"
        FROM "${config.TIMESTREAM_DATABASE_NAME}"."${config.TIMESTREAM_TABLE_NAME}"
        WHERE time between ago(${hoursBehind}h) and now() and site = 'showdown'
        GROUP BY site, page_url
        ORDER BY views DESC, page_url ASC
        LIMIT 1000
      `,
    },
    {
      name: "First 1k rows", //Both can only do 1k retrieval without paginating
      athenaQuery: `
        SELECT * 
        FROM page_views
        WHERE site = 'showdown' AND page_opened_at_date = '${page_opened_at_date}'
              AND page_opened_at BETWEEN now() - interval '${hoursBehind}' hour AND now()
        ORDER BY page_opened_at DESC
        LIMIT 1000
      `,
      timestreamQuery: `
        SELECT *
        FROM "${config.TIMESTREAM_DATABASE_NAME}"."${config.TIMESTREAM_TABLE_NAME}"
        WHERE time between ago(${hoursBehind}h) and now() and site = 'showdown'
        ORDER BY time DESC
        LIMIT 1000
      `,
    },
  ];

  for(const test of tests) {
    log("");
    log("=====================================================================================================");
    log("Running test: " + test.name + "\n" +
      "Athena: " + test.athenaQuery + "\n" +
      "Timestream: " + test.timestreamQuery);

    const runs = 10;
    const athenaTimes: number[] = [];
    const tsTimes: number[] = [];
    for (let i = 0; i < runs; i++) {
      // log("Running queries: " + i + "/" + runs+")");
      const resp = await query(test.athenaQuery, test.timestreamQuery);
      athenaTimes.push(resp.athenaResp.queryAndResultRetrievalTime);
      tsTimes.push(resp.tsResponse.tsQueryTime);
    }

    const queryMetrics = {
      Athena: calculateMetrics(athenaTimes),
      TimeStream: calculateMetrics(tsTimes),
    }
    console.table(queryMetrics);
  }
}

async function main()
{
  await loadData(); //comment out to run queries only
  await measureQueryPerformance();
}
main().catch((err) => {
  console.error(err);
  process.exit(1);
});