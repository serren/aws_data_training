# Data model

## Metrics stream

The metrics events coming to the Kinesis stream may be viewed as the following JSON document:
```json5
{
 // the component produced the event
 "componentName": "user-service",
 // the name of the metric
 "metricName": "cpu-usage",
 // measurement unit
 "unit": "percent",
 // measurement value
 "value": 32,
 // the event publication time according
 // to the component's local clock
 "publicationTimestamp": "2021-09-09T12:15:02.001Z"
}
```

The same model applies to the S3 metrics Parquet bucket.

## Windowed metrics

The windowed metrics dataset represents an aggregation of the metrics stream in windows of 5-minute length. They can be viewed as the following JSON document:
```json5
{
 // the component produced the event
 "componentName": "user-service",
 // the name of the metric
 "metricName": "cpu-usage",
 // measurement unit
 "unit": "percent",
 // measurement values: min and max
 "minValue": 10,
 "maxValue": 23,
 // the time which the window is computed for
 "fromTimestamp": "2021-09-09T12:15:02.001Z",
  "toTimestamp": "2021-09-09T12:15:07.001Z"
}
```
## Server access logs

The server access logs as follows:
```nginx
66.249.65.159 - - [06/Nov/2014:19:10:38 +0600] "GET /user-service/api/user/123 HTTP/1.1" 404 "Mozilla/5.0 (iPhone; CPU iPhone OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A5376e Safari/8536.25 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"

66.249.65.3 - order-service [06/Nov/2014:19:11:24 +0600] "GET /user-service/api/users/234 HTTP/1.1" 200 "Machine (Apache-Http-Client)"

66.249.65.62 - - [06/Nov/2014:19:12:14 +0600] "DELETE /order-service/api/orders/abc HTTP/1.1" 200 "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
```

In fact, the logs correspond to the default (aka combined) Nginx log format:
```nginx
'$remote_addr - $remote_user [$time_local] '
    '"$request" $status "$http_user_agent"'
```

More details on the format may be found [here](https://nginx.org/en/docs/http/ngx_http_log_module.html#access_log) and full description of the fields may be found [here](https://nginx.org/en/docs/http/ngx_http_core_module.html#variables).

**NOTES:**
* The request line (HTTP method + URL) assumes that the URL begins with the name of the service hosting the API being invoked.
* The user agent field may be either
    * `Machine (<HTTP library name>; <service name>)` - where the _service name_ reflects the unique name of the invoking service
    * anything that doesn’t start with _Machine_ refers to calls from external users
## Server traffic report

The server traffic report is recommended to have the following structure if viewed as a JSON document:
```json5
{
 // an alphanumeric string, should be unique per service pair
 "id": "asbkhgl2598y3944t",
 // the name of the source (requesting) service
 "source": "user-service",
 // the name of the target (responding) service
 "target": "auth-service",
 // the total number of requests done in 24 hours
 "totalRequests": 1025
}
```
**Note:** The details on why this structure is recommended may be found in the _EMR daily traffic report job_ sub-task.