# pysec_downloader
 downloader for sec filings and other data available from the sec

 install:
    
    pip install pysec_downloader
    

 supports most filings, needs a lot of refining still.
 exposes some of the sec xbrl api.
 self updating lookup table for ticker:cik so we can search xbrl api with ticker instead of only cik.
 not async as the rate limit of the sec is quite low so the benefit for the added complexity is minimal (correct me if I am wrong).

no tests at the moment.

 usage:
    # make sure you have needed permission for the root_path


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
    
    # if the number_of_filings is large you might consider using get_filings_bulk() instead
    # of get_filings()

    file = dl.get_xbrl_companyconcept("AAPL", "us-gaap", "AccountsPayableCurrent") 

    other_file = dl.get_file_company_tickers()

    # calling get_bulk_submissions downloads >10GB of files
    dl.get_bulk_submissions()
    # if you dont know the cik call dl._convert_to_cik10(ticker) to get it
    # check if S-3's were filed after "2020-01-01" and get the info to donwload them
    newfiles = dl.index_handler.get_newer_filings_meta("0001718405", "2020-01-01", set(["S-3"]))
    for key, values in newfiles.items():
        for v in values:
            dl.get_filing_by_accession_number(key, *v)


