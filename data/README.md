# Data

The scripts here fetch data from various locations and, by default, insert it into a database. Alternatively, CSV files can be created.

First, create, activate, and setup up a virtual environment:

```
$ python3 -m venv ve
$ . ve/bin/activate
$ pip install -r requirements.txt
```

If inserting data into a database, create a Postgres database named "econ_data" and run the create_tables.sql script to create the necessary tables within it. Next, create a file named "config.py" (which is ignored via the .gitignore file) in this directory (data/) and include the database connection string in it:

```python
PG_CREDS = "postgres://your_username:yourpassword@localhost:port/econ_data"
```

To instead create CSV files from the data, you can pass the `--csv` flag on the command line, e.g. `python3 unemployment.py --csv`. It's not necessary to set up the database or include the `PG_CREDS` variable in config.py if you only want to create CSVs.

For those scripts that use the BLS API (all but housing.py), an API key is necessary if running them more than a handful of times (due to rate limiting). This shouldn't be an issue normally, but if this is actively being developed/tested and you are running one of the scripts repeatedly, you will likely need to use an API key. See <https://www.bls.gov/developers/> to get one, and then add it to the config.py file:

```python
BLS_API_KEY = "your_API_key_goes_within_these_quotes"
```

For a quick reference to the data returned by the BLS API, add the series at the end of this URL: <https://api.bls.gov/publicAPI/v2/timeseries/data/SERIES_GOES_HERE>.

Information about the sources of the BLS data:
  * CPI: <https://www.bls.gov/cpi/overview.htm>
  * State and Metro Area Employment, Hours, & Earnings: <https://www.bls.gov/sae/>
  * CPS: <https://www.bls.gov/cps/>
  * Local Area Unemployment Statistics: <https://www.bls.gov/lau/>
  * Further information on BLS data series can be found here: <https://www.bls.gov/help/hlpforma.htm>
