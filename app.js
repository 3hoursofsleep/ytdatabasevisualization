require('dotenv').config()

const express = require('express');
const app = express();
const port = process.env.PORT || 4000;
const aws = require("aws-sdk");
const AthenaExpress = require('athena-express');
const aq = require('arquero');
const d3n = require('d3-node');

const awsCredentials = {
    region: process.env.awsRegion,
    accessKeyId: process.env.awsKeyId,
    secretAccessKey: process.env.awsAccessKey
};
aws.config.update(awsCredentials);

const athenaExpressConfig = { aws };
const athenaExpress = new AthenaExpress(athenaExpressConfig);

app.set('view engine', 'pug');

getAWSQuery = async query => {
    try{
        return await athenaExpress.query(query);
    } catch (error) {
        console.log(error); //DO NOT USE IN PRODUCTION
    }
}

// test_table = aq.from(
//     (await (d3.csv(getAWSQuery('SELECT * FROM "youtube-database"."metadata" limit 10'))))
// );

let query = `SELECT *
FROM (
    SELECT viewduration, 
        regexp_extract(url,'youtu(?:.*\/v\/|.*v\=|\.be\/)([A-Za-z0-9_\-]{11})', 1) AS id
    FROM "nielsen-single-crawler"."nol_url"
    WHERE month = '7' AND year = '2021'
    AND url LIKE '%youtube.com/watch?v=%'
    AND regexp_extract(url,'youtu(?:.*\/v\/|.*v\=|\.be\/)([A-Za-z0-9_\-]{11})', 1)  IS NOT NULL
) AS nielsen
INNER JOIN "youtube-database"."metadata" AS youtube ON nielsen.id = youtube.vid`

app.get('/', async (req, res) => {
    res.send(aq.from( (await getAWSQuery(query))['Items']).toHTML());
    //res.render('test');
});

app.listen(port, () => {
    console.log(`App listening at http://localhost:${port}`)
});