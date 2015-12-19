![Logo](admin/influxdb.png)
# ioBroker.influxdb

This adapter saves state history into InfluxDB.

## Installation of InfluxDB
There is no InfluxDB for Windows!
Under debian you can install it with:
```
sudo apt-get update
sudo apt-get install influxdb
```

Explanation for other OS can be found [here](https://influxdb.com/docs/v0.9/introduction/installation.html).

### Setup authentication for influxDB (can be omitted)
If you use DB localy you may leave authentication disabled and skip this part.

- Start service: ``` service influxdb start ```
- Go to admin page: http://<ip>:8083
- Create users:
```
CREATE USER "admin" WITH PASSWORD '<adminpassword>' WITH ALL PRIVILEGES
CREATE USER "user" WITH PASSWORD '<userpassword>'
CREATE DATABASE "iobroker"
GRANT ALL ON "iobroker" TO "user"
```
Enable authentication, by editing /etc/influxdb/influxdb.conf:
```
 [http]  
 enabled = true  
 bind-address = ":8086"  
 auth-enabled = true # ✨
 log-enabled = true  
 write-tracing = false  
 pprof-enabled = false  
 https-enabled = false  
 https-certificate = "/etc/ssl/influxdb.pem"  
```
- Restart service: ``` service influxdb restart ```



## Installation of Grafana
There is additional charting tool for InfluxDB - Grafana. 
It must be installed additionally.

Under debian you can install it with:

```
$ wget https://grafanarel.s3.amazonaws.com/builds/grafana_2.5.0_amd64.deb
$ sudo apt-get install -y adduser libfontconfig
$ sudo dpkg -i grafana_2.5.0_amd64.deb
```

Explanation for other OS can be found [here](http://docs.grafana.org/installation/).

After the Grafana is installed, follow [this](http://docs.grafana.org/datasources/influxdb/) to create connection. 

## Changelog
### 0.1.2 (2015-12-19)
* (bluefox) make onchange work

### 0.1.1 (2015-12-19)
* (bluefox) retention policy for 0.9.x

### 0.1.0 (2015-12-19)
* (bluefox) supported InfluxDB version 0.9.x and 0.8.x

### 0.0.2 (2015-12-14)
* (bluefox) change supported InfluxDB version to 0.9.x

### 0.0.1 (2015-12-12)
* (bluefox) initial commit

## License

The MIT License (MIT)

Copyright (c) 2015 bluefox

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
