## MDS Half Hourly Data Formatter

MDS want Half Hourly data in a very specific txt format. An example of the data/ format they want can be seen in `output_template.txt`. The guidence for this format comes from the bsc guidelines. It is on page 25, section 4.1.5 of `bscp510_v3.0.pdf`.

### Known Stipulations
- MDS are only concerned with a specific set of mpans.
- The site must still be on supply otherwise don't continue to submit the data
- Initial run is from `2019-09-01` until `2020-03-31`.
- Going fowards data must be submitted by the `10th` of each month for the previous month

### Current Unknowns
- Some of the sites only recieved half hourly data inbetween September and now. How should these large gaps be treated? Currently we are leaving them in as blank records.
- If a days worth of data should that day-record be left out? Currently we are leaving them in as blank records
- If half-hourly periods are missing should the data be estimated or left blank? Currently they are blank records
---

### Config
It expects a config file as per `example_config`. The site list is a string, not a python list type. `start_date` and `end_date` represent the window of time that the report is intending to capture. `cred_path` is the the path to your GCP service_account json key file. `run_staging_table` should be `True` unless there is a problem with the data and you only need to rerun later parts. This is just because the staging table is very expensive.

### Issues and improvements
- The hh data source `v_hh_power_materialised` is very expensive to query. I wish there was a source that was paritioned on reading date
