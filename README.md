![Logo](admin/influxdb.png)
# ioBroker.influxdb

This adapter saves state history into InfluxDB. 
There is additional charting tool for InfluxDB - Grafana. 
It must be installed additionally.

## Installation of InfluxDB
There is no InfluxDB for Windows!
Under debian you can install it with:
```
sudo apt-get update
sudo apt-get install influxdb
```

Explanation for other OS can be found [here](https://influxdb.com/docs/v0.9/introduction/installation.html).

## Installation of Grafana
Under debian you can install it with:

```
$ wget https://grafanarel.s3.amazonaws.com/builds/grafana_2.5.0_amd64.deb
$ sudo apt-get install -y adduser libfontconfig
$ sudo dpkg -i grafana_2.5.0_amd64.deb
```

Explanation for other OS can be found [here](http://docs.grafana.org/installation/).

After the Grafana is installed, follow [this](http://docs.grafana.org/datasources/influxdb/) to create connection. 

## Changelog

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
