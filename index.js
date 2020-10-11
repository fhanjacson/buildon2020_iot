const AWS = require('aws-sdk');

AWS.config.update({
    region: "ap-southeast-1"
});

var config = {
    interval: 1000 * 60 * 1,
    threshold_covid19_social_distancing: 9
}

exports.handler = async(event) => {
    const db = new AWS.DynamoDB.DocumentClient({ region: "ap-southeast-1" });
    var listOfDeviceID = ["CafeNorth", "CafeSouth", "L1-A", "L1-B", "L1-C", "L2-A", "L2-B", "L2-C"]

    const currentTimestamp = Date.now();
    try {

        // var params1 = {
        //     TableName: "sensor_aggregated",
        //     ScanIndexForward: false,
        //     Limit: 1,
        //     ProjectionExpression: "device_id, #timestamp",
        //     ExpressionAttributeNames: {
        //         "#timestamp": "timestamp"
        //     },
        // }

        // var result1 = await db.scan(params1).promise();

        // if (result1.Count === 0) {
        // MULTIPLE QUERY INSTEAD OF SCAN
        var params2 = {
            TableName: "sensor_log",
            // ScanIndexForward: false,
            // Limit: 1,
            ProjectionExpression: "device_id, #timestamp, payload",
            ExpressionAttributeNames: {
                "#timestamp": "timestamp"
            },
            FilterExpression: "#timestamp between :start_timestamp and :end_timestamp",
            ExpressionAttributeValues: {
                ":start_timestamp": currentTimestamp - config.interval,
                ":end_timestamp": currentTimestamp
            }
        }

        var result2 = await db.scan(params2).promise();
        console.log(result2)


        if (result2.Count > 0) {
            // const uniqueDeviceID = [...new Set(result2.Items.map(item => item.device_id))];
            // console.log(uniqueDeviceID)

            var covid19SocialDistancingTrigger = [];

            for (let location of listOfDeviceID) {
                // console.log(location)
                var totalNearbyDevice = 0;

                var params3 = {
                    TableName: 'sensor_log',
                    Limit: 1,
                    ScanIndexForward: false,
                    // IndexName: 'device_id',
                    KeyConditionExpression: 'device_id = :location AND #timestamp between :start_timestamp and :end_timestamp',
                    ProjectionExpression: "device_id, #timestamp, payload",
                    ExpressionAttributeNames: {
                        "#timestamp": "timestamp"
                    },
                    // FilterExpression: ,
                    ExpressionAttributeValues: {
                        ':location': location,
                        ":start_timestamp": currentTimestamp - config.interval,
                        ":end_timestamp": currentTimestamp
                    }

                }

                var result3 = await db.query(params3).promise();
                // console.log('result3', result3)
                if (result3.Count === 1) {
                    console.log(result3.Items[0].device_id + ' detected ' + result3.Items[0].payload.nearby_device.length + ' surrounding device(s) at ' + result3.Items[0].timestamp + ' timestamp')
                    totalNearbyDevice = result3.Items[0].payload.nearby_device.length;

                    if (result3.Items[0].payload.nearby_device.length >= config.threshold_covid19_social_distancing) {
                        console.log('threshold_covid19_social_distancing triggered, sending SNS')
                        covid19SocialDistancingTrigger.push({ location, totalNearbyDevice })
                    }
                }
                else {
                    console.log(location + ' did not detect anything')
                }
                var params4 = {
                    TableName: 'sensor_aggregated',
                    Item: {
                        device_id: location,
                        // timestamp: currentTimestamp,
                        totalNearbyDevice: totalNearbyDevice
                        // MapAttribute: {
                        //     timestamp: currentTimestamp,
                        //     totalNearbyDevice: result3.Items[0].payload.nearby_device.length
                        // }
                    }
                }

                var result4 = await db.put(params4).promise();


            }

            await publish2CovidSensorTopic(covid19SocialDistancingTrigger)



            //     let groupedItem = result2.Items.reduce((r, a) => {
            //     //  console.log("a", a);
            //     //  console.log('r', r);
            //      r[a.device_id] = [...r[a.device_id] || [], a];
            //      return r;
            //     }, {});
            //     // console.log("group", groupedItem.groupedItem[0].toString());


            //     // var groupedItem = lodash.groupBy(result2.Items, 'payload.device_id')
        }
        else {
            console.log("no new data from sensor_log")
        }

        // }
        // else {
        //     console.log("no data in sensor_aggregated")
        // }

    }
    catch (error) {
        console.error(error)
    }

};


async function publish2CovidSensorTopic(covid19SocialDistancingTrigger) {
    if (covid19SocialDistancingTrigger.length > 0) {
        var message = '';
        for (alert of covid19SocialDistancingTrigger) {
            message += 'Social Distancing Alert at ' + alert.location + ', ' + alert.totalNearbyDevice + ' devices detected in close proximity.\n\n'
        }

        console.log(message)

        console.log('publishToSNSTopic')
        var sns = new AWS.SNS();
        var params = {
            Message: message,
            Subject: '[ALERT] COVID-19 Social Distancing',
            TopicArn: "arn:aws:sns:ap-southeast-1:591634949024:CovidSensorTopic"
        };
        try {
            console.log('sns')
            var snsResult = await sns.publish(params).promise();
            console.log('snsResult', snsResult)
        }
        catch (error) {
            console.log('error', error)
        }
    }
}
