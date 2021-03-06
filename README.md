# pysec_downloader
 downloader for sec filings and other data available from the sec

 install:
    
    pip install pysec_downloader
    
Features:
 supports most filings, needs a lot of refining still.
 exposes some of the sec xbrl api.
 self updating lookup table for ticker:cik so we can search xbrl api with ticker instead of only cik.
 not async as the rate limit of the sec is quite low so the benefit for the added complexity is minimal (correct me if I am wrong).

no tests at the moment.

## usage:


```python
# Make sure you have needed permission for the root_path!
# Instantiate the Downloader and download some 10-Q Filings as XBRL for AAPL
dl = Downloader(r"C:\Users\Download_Folder", user_agent="john smith js@test.com")
dl.get_filings(
    ticker_or_cik="AAPL",
    form_type="10-Q",
    after_date="2019-01-01",
    before_date="",
    prefered_file_type="xbrl",
    number_of_filings=10,
    want_amendments=False,
    skip_not_prefered_extension=True,
    save=True)

# if the `number_of_filings` is large you might consider using `get_filings_bulk()` 
# instead of `get_filings()` for a more efficent index creation.

# get Facts (individual values) from a single Concept ("AccountPayableCurrent") of a Taxonomy ("us-gaap")
facts_file = dl.get_xbrl_companyconcept("AAPL", "us-gaap", "AccountsPayableCurrent")

# download the zip containing all information on submissions of every company and extract it
# Calling `get_bulk_submissions` or `get_bulk_companyfacts` downloads >10GB of files!
dl.get_bulk_submissions()

# get the company-ticker map/file 
other_file = dl.get_file_company_tickers()


# download the most current 13f securities pdf
dl.get_13f_securities_pdf(path_to/save_as.pdf)
# get a byte reprensentation of the pdf without saving it
dl.get_13f_securities_pdf(target_path=None)
```
easy way to convert the 13f securities pdf into a usuable dataframe/list -> [tabula-py](https://github.com/chezou/tabula-py)


```python
# check if S-3's were filed after "2020-01-01", get the submission info and download them.

newfiles = dl.index_handler.get_newer_filings_meta("0001718405", "2020-01-01", set(["S-3"]))
for key, values in newfiles.items():
    for v in values:
        dl.get_filing_by_accession_number(key, *v)
# If you dont know the CIK call `dl._convert_to_cik10(ticker)` to get it

# check the index for none existing files and remove the entries from the index
dl.index_handler.check_index()

# get index entry of downloaded filings with the same file number
dl.index_handler.get_related_filings("some cik", "some file number")

```
