# Deep Dive into the IoT Backend Ecosystem

IoT Backend Providers are the companies that offer specialized infrastructure and services to support IoT Devices. The
Internet-facing part of their infrastructure that is responsible to communicate with IoT devices is called IoT Backend.

To learn more about the IoT Backend Providers, characterization of their infrastructure and traffic, check out our [paper](https://dl.acm.org/doi/10.1145/3517745.3561431) appeared at [Internet Measurement Conference (IMC'22)](https://conferences.sigcomm.org/imc/2022/accepted/).

This repository contains the patterns, regular expressions and the source code to extract the domain names and IP
addresses of studied IoT backends from DNSDB and Censys datasets. 



## Patterns and Domains

The regular expressions and domain patterns are under data/external/top16*.csv

## Install

You should have python3.9+ installed
`python3 -m pip install virtualenv 
virtualenv venv 
source venv/bin/activate
python3 -m pip install -r requirements.txt 
python3 setup install`

## Looking up Patterns in DNSDB Dataset

1. Obtain a DNSDB API Key with sufficient number of queries.
2. Write the API Key in configs/data/dnsdb/query_dnsdb_*env files.
3. For Basic API queries
   run `python3 src/data/dnsdb/query_dnsdb_basic.py configs/data/dnsdb/20220228/query_dnsdb_basic_round1_env`
4. For Flexible API queries
   run `python3 src/data/dnsdb/query_dnsdb_flex_regex.py configs/data/dnsdb/20220228/query_dnsdb_flexible_round1_env`

Note that CNAMEs records point to other domains, this means that you may need to extract those domain names and repeat
the above process.

Post-process the downloaded DNSDB records as following:

- For Amazon AWS, only the direct sub-domains of iot.<region>.amazonaws.com are valid.
- For Bosch, discard the domains that contain "doc.", these are documentation domains.
- For siemens, discard the domains that contains "static" and "edge", these point to CDNs.
- For Alibaba, domains that contain "link.aliyuncs.com" should also have the keyword "coap"

## Looking up Patterns in Censys Dataset

Censys typically allows you to look into their datasets using the Google Cloud BigQuery. If you want to process their
data in your local machine, you can export the Censys data and download them as json files or other formats.

1. Download the Censys data in json.gzip format.
2. Change the INPUT_FILE_PATH value in the files in configs/data/censys/ to point to the downloaded censys data path.

- `python3 src/data/censys/search_domains_in_censys_certs.py configs/data/censys/search_domains_in_censys_certs_20220228_env`
- `python3 src/data/censys/search_patterns_in_censys_certs.py configs/data/censys/search_patterns_in_censys_certs_20220228_env`

Make sure that you post-process the extracted censys data as following, use pandas or any other tool:

- Filter out any record from SAP that contains the keyword "sapns2"
- For Amazon AWS only keep the records that contain '\*.iot'
- For Bosch, only the records that contains 'bosch-iot-hub.com'
- For Siemens, only records that contain one of the following strings 'cn1.mindsphere-in.cn', 'eu2.mindsphere.io','
  eu1.mindsphere.io'
