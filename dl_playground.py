from downloader import Downloader

dl = Downloader(r"C:\Users\Olivi\Testing\sec_scraping\pysec_downloader\dummy_folder", user_agent="john smith js@test.com")

print(dl._lookuptable_ticker_cik.items()[0:100])

# to_get = [
#     ["8-K", "xml", True],
#     ["8-K", "txt", False],
#     ["8-K", "htm", False],
#     ["8-K", "xbrl", True]
# ]

# for each in to_get:
#     print(f"each: {each}")
#     for f in dl.get_filings(
#         ticker_or_cik="AAPL",
#         form_type=each[0],
#         after_date="2019-01-01",
#         before_date="",
#         prefered_file_type=each[1],
#         number_of_filings=1,
#         want_amendments=False,
#         skip_not_prefered=each[2],
#         save=False):
#         pass