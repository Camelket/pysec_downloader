'''
Aim: Be able to download any SEC file type in every(as long as available
 on website) file format for every filer identified either by
 CIK or ticker symbol and found on the ticker:cik map of the sec. 
 While respecting the sec guidelines:
  https://www.sec.gov/privacy.htm#security
  
  the most important are:
    - respect rate limit
    - have user_agent header
    - stress sec systems as little as possible

Sec information:
    https://www.sec.gov/os/accessing-edgar-data
    https://www.sec.gov/os/webmaster-faq#developers

    
Things to add:
    * splitting a full text submission and writting individual files (will need to change how the indexes)
    



'''
from bs4 import BeautifulSoup
import requests
import json
from urllib3.util import Retry
import time
from functools import wraps
import logging
from posixpath import join as urljoin
from pathlib import Path
from os import path
from urllib.parse import urlparse
from zipfile import BadZipFile, ZipFile
from csv import writer
from ._constants import *

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

debug = False
if debug is True:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)
    

r'''download interface for various SEC files.


    usage:
    
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
    '''

class IndexHandler:
    '''create, add to and query the index for files donwloaded with Downloader
    
    Attributes:
        root_path: root path of the files, 
                   should be the same as specified for Downloader
    '''

    def __init__(self, root_path):
        self.root_path = self._prepare_root_path(root_path)
        self._checked_index_creation = False
        self._base_index_path = self.root_path / "index" / "base_index"
        self._file_num_index_path = self.root_path / "index" / "file_num_index"

    def _get_base_index_path(self, cik10):
        return self._base_index_path / cik10 +".csv"
    
    def _get_file_num_index_path(self, cik10):
        return self._file_num_index_path / cik10 +".json"

    def _prepare_root_path(self, path, create_folder=True):
        if not isinstance(path, Path) and isinstance(path, str):
            path = Path(path)
        if isinstance(path, Path):
            if create_folder is True:
                if not path.exists():
                    path.mkdir(parents=True)
            elif create_folder is False:
                if not path.exists():
                    raise OSError("root_path doesnt exist")
            return path
        else:
            raise ValueError(f"root_path is expect to be of type str or pathlib.Path, got type: {type(path)}")

    def get_newer_filings_meta(self, cik, after: str, tracked_filings: set = None):
        '''check a submission file and get filings newer than 'after'.
        
        only works if you have downloaded the bulk submissions file!
        Downloader -> get_bulk_submissions()
        
        Args:
            path: str or pathlike object
            after: format yyyy-mm-dd
            tracked_filings: set of form types, eg: set(["S-3", "S-1"])
        '''
        if len(cik) < 10:
            cik = cik.zfill(10)
        path = self.root_path / "submissions" / ("CIK" + cik + ".json") 
        new_filings = {}
        with open(path, "r") as f:
            j = json.load(f)
            stop_idx = None
            try:
                filing_dates = j["filings"]["recent"]["filingDate"]
                len_f_dates = len(filing_dates)

                for r in range(0, len_f_dates, 1):
                    if filing_dates[r] <= after:
                        if r == len_f_dates:
                            break
                        stop_idx = r
                        break
                if stop_idx is not None:
                    new_filings[j["cik"]] = []
                    filing = j["filings"]["recent"]
                    for idx in range(0, stop_idx, 1):
                        if (filing["form"][idx] in tracked_filings) or (tracked_filings is None):
                            new_filings[j["cik"]].append(
                               [filing["form"][idx],
                                filing["accessionNumber"][idx].replace("-", ""),
                                filing["primaryDocument"][idx],
                                filing["filingDate"][idx],
                                [filing["fileNumber"][idx]]])
            except KeyError as e:
                raise e
                pass #second filing not handled yet
            finally:
                del j
        return new_filings
    

    def get_related_filings(self, cik: str, file_number: str):
        '''check the file number index for other filings with file_number
        
        Args:
            cik: cik with leading 0's
            file_number: the file number
        Returns:
            a list of lists where each of those lists has the fields:
                form_type, relative_file_path, filing_date    
        '''
        with open(self._get_file_num_index_path(cik), "r") as f:
            index = json.load(f)
            return index[file_number]


    def check_index(self):
        '''check the index and remove none existant entries.
        
        check if files listed in the index are present and
        if there are duplicate values in the base_index.'''
        # get all base indexes
        import pandas as pd
        base_indexes = [p for p in self._base_index_path.glob("*.csv")]
        base_remove_count = 0
        num_remove_count = 0
        for b in base_indexes:
            original = pd.read_csv(b, delimiter=",")
            # remove duplicates
            df = original.drop_duplicates()
            # load the file_num_index
            num_index_path = self._file_num_index_path / b.name.replace(".csv", ".json")
            with open(num_index_path, "r+") as file_num_index:
                num_changed =  False
                base_changed = False
                num_index = json.load(file_num_index)
                drop_rows = []
                for row in df.iloc:
                    if not (self.root_path / Path(row["file_path"])).exists():
                        # remove entry from file_num_index
                        try:
                            num_obj = num_index[row["file_number"]]
                            drop_idx = None
                            for idx, item in enumerate(num_obj):
                                if item[1] == row["file_path"]:
                                    drop_idx = idx
                                    break
                            if drop_idx:
                                num_changed = True
                                num_obj.pop(drop_idx)
                                num_index[row["file_number"]] = num_obj
                                num_remove_count += 1
                        except KeyError as e:
                            logger.debug(f"{e} when trying to remove entry from file_number_index, row in base_index: {row}")

                        base_changed = True
                        drop_rows.append(row.name)    
                # remove invalid rows from dataframe/base_index and rewrite files if needed
                if base_changed is True:
                    base_remove_count += len(drop_rows)
                    df = df.drop(drop_rows)
                    df.to_csv(b)
                    logger.debug(f"changed base_index: {b}")
                    if num_changed is True:
                        json.dump(num_index, file_num_index)
                        logger.debug(f"changed file_num_index: {num_index_path}")
        logger.info((f"completed check of indexes \n"
                     f"base_index: {base_remove_count} entries removed \n"
                     f"num_index: {num_remove_count} entries removed \n"))
    
    
    def _create_indexes(self, cik, form_type, accn, file_name, file_num, filing_date):
        # need to fix rewritting whole json every time, how could i use seek()?
        '''create index files or add to them. accession number is included in the file_path'''
        if self._checked_index_creation is False:
            if not self._base_index_path.exists():
                self._base_index_path.mkdir(parents=True)
            if not self._file_num_index_path.exists():
                self._file_num_index_path.mkdir(parents=True)
            self._checked_index_creation = True
        rel_file_path = path.join(cik, form_type, accn, file_name)
        base_path = self._get_base_index_path(cik)
        file_num_path = self._get_file_num_index_path(cik)
        base_path_row = [form_type, file_num, rel_file_path, filing_date]
        file_num_row = [form_type, rel_file_path, filing_date]

        with open(base_path, "a", newline="") as f:
            if base_path.stat().st_size == 0:
                base_header = ["form_type", "file_number", "file_path", "filing_date"]
                writer(f).writerow(base_header)
            writer(f).writerow(base_path_row)
        content = None
        try:
            with open(file_num_path, "r") as f:
                content = json.load(f)
        except FileNotFoundError:
            with open(file_num_path, "w") as f:
                content = {}
        try:
            content[file_num].append(file_num_row)
        except KeyError:
            content[file_num] = []
            content[file_num].append(file_num_row)
        with open(file_num_path, "w") as f:
            json.dump(content, f)
        
                
    def _create_indexes_bulk(self, cik:str, items: list[list]):
        '''creates or adds all the entries in items to the indexes
        
        Args:
            cik:  a central index key (10 character form/zfilled) eg: 0000234323
            items: list of entries like so: [[form_type, accn, file_name, file_num, filing_date]] '''
        base_path = self._get_base_index_path(cik)
        file_num_path = self._get_file_num_index_path(cik)
        file_num_content = None
        try:
            with open(file_num_path, "r") as num_file:
                file_num_content = json.load(num_file)
        except FileNotFoundError:
            file_num_content = {}
        with open(base_path, "a", newline="") as base_file:
            if base_path.stat().st_size == 0:
                base_header = ["form_type", "file_number", "file_path", "filing_date"]
                writer(base_file).writerow(base_header)
            for item in items:
                rel_file_path = path.join(cik, item[0], item[1], item[2])
                base_path_row = [item[0], item[1], rel_file_path, item[4]]
                file_num_row = [item[0], rel_file_path, item[4]]
                writer(base_file).writerow(base_path_row)
                try:
                    file_num_content[item[1]].append(file_num_row)
                except KeyError:
                    file_num_content[item[1]] = []
                    file_num_content[item[1]].append(file_num_row)
        with open(file_num_path, "w") as num_file:
            json.dump(file_num_content, num_file)
        
                


        
    
    def _get_base_index_path(self, cik):
        return self._base_index_path / (str(cik)+".csv")
    
    def _get_file_num_index_path(self, cik):
        return self._file_num_index_path / (str(cik)+".json")
    

class Downloader:
    '''suit to download various files from the sec
    
    enables easier access to download files from the sec. tries to follow the
    SEC guidelines concerning automated access (AFAIK, if I missed something
    let me know: camelket.develop@gmail.com).

    the filing index for get_filings() doesnt account for the content of
    extracted zip files and instead adds a link to the .zip file!

    Attributes:
        root_path: where to save the downloaded files unless the method has
                   an argument to specify an alternative
        user_agent: str of 'name surname email' to comply with sec guidelines
    Args:
        retries: how many retries per request are allowed
        create_folder: if root folder should be created, parents included if
                       it doesnt exist.
    
    Raises:
        OsError: if root_path doesnt exist and create_folder is False
        ValueError: if root_path isnt correct type (allowed: str, pathlib.Path)
    '''
    def __init__(self, root_path: str, retries: int = 10, user_agent: str = None, create_folder=True):
        self.user_agent = user_agent if user_agent else "maxi musterman max@muster.com"
        self._is_ratelimiting = True
        self.root_path = self._prepare_root_path(root_path)
        self._next_try_systime_ms = self._get_systime_ms()
        self._session = self._create_session(retry=retries)
        self._sec_files_headers = self._construct_sec_files_headers()
        self._sec_xbrl_api_headers = self._construct_sec_xbrl_api_headers()
        self._lookuptable_ticker_cik = self._load_or_update_lookuptable_ticker_cik()
        self.index_handler = IndexHandler(root_path)
        self._download_counter = 0
        self._current_ticker = None
        
    
    def _prepare_root_path(self, path, create_folder=True):
        if not isinstance(path, Path) and isinstance(path, str):
            path = Path(path)
        if isinstance(path, Path):
            if create_folder is True:
                if not path.exists():
                    path.mkdir(parents=True)
            elif create_folder is False:
                if not path.exists():
                    raise OSError("root_path doesnt exist")
            return path
        else:
            raise ValueError(f"root_path is expect to be of type str or pathlib.Path, got type: {type(path)}")

    def get_filing_by_accession_number(self, cik, form_type, accession_number, save_name, filing_date, file_nums, save=True, create_index=True, extract_zip=True):
        logger.debug(f"\n Called get_filing_by_accession_number with args: {locals()}")
        base_url = urljoin(EDGAR_ARCHIVES_BASE_URL, cik)
        file_url = urljoin(base_url, accession_number, save_name)
        logger.debug(f"file_url: {file_url}")
        file, _ = self._download_filing(file_url, skip=False, fallback_url=None)
        if Path(save_name).suffix == "htm":
            file = self._resolve_relative_urls(file, base_url)
        if save is True:
                if file:
                    self._save_filing(cik, form_type, accession_number, save_name, file, extract_zip=extract_zip)
                    if (create_index is True) and (file_nums is not None):
                        for file_num in file_nums:
                            self.index_handler._create_indexes(cik, form_type, accession_number, save_name, file_num, filing_date)
                else:
                    logger.debug("didnt save/get filing despite that it should have. file was None")
        

    def get_filings_bulk(
        self,
        ticker_or_cik: str,
        form_type: str,
        after_date: str = "",
        before_date: str = "",
        query: str = "",
        prefered_file_type: str = "",
        number_of_filings: int = 100,
        want_amendments: bool = True,
        skip_not_prefered_extension: bool = False,
        save: bool = True,
        extract_zip = True,
        create_index = True,
        resolve_urls: bool = True,
        callback = None):
        '''download filings.  EXPERIMENTAL.

        unlike get_filings this will write to the indexes in bulk after downloading
        all of the files, usefull if you want to download a lot of files in one go
        and reduce the time loss associated with open/closing the base index and
        rewritting the file_num index every time.
        
        Args:
            ticker_or_cik: either a ticker symbol "AAPL" or a 10digit cik
            form_type: what form you want. valid forms are found in SUPPORTED_FILINGS
            after_date: date after which to consider filings
            before_date: date before which to consider filings
            query: query according to https://www.sec.gov/edgar/search/efts-faq.html.
            prefered_file_type: what filetype to prefer when looking for filings, see PREFERED_FILE_TYPES for handled extensions
            number_of_filings: how many filings to download.
            want_amendements: if we want to include amendment files or not
            skip_not_prefered_extension: either download or exclude if prefered_file_type
                               fails to match/download
            save: toggles saving the files downloaded.
            resolve_url: resolves relative urls to absolute ones in "htm"/"html" files 
            callback: pass a function that expects a dict of {"file": file, "meta": meta}
                      meta includes the metadata.
        '''
        # set these for info at end
        self._current_ticker = ticker_or_cik
        self._download_counter = 0

        cik10 = self._convert_to_cik10(ticker_or_cik)
        if prefered_file_type == (None or ""):
            if form_type not in PREFERED_FILE_TYPE_MAP.keys():
                logger.info(f"No Default file_type set for this form_type: {form_type}. defaulting to 'htm'")
                prefered_file_type = "htm"
            else:
                prefered_file_type = PREFERED_FILE_TYPE_MAP[form_type] 
                                
        logger.debug((f"\n Called get_filings with args: {locals()}"))
        hits = self._json_from_search_api(
            ticker_or_cik=cik10,
            form_type=form_type,
            number_of_filings=number_of_filings,
            want_amendments=want_amendments,
            after_date=after_date,
            before_date=before_date,
            query=query)
        if not hits:
            logger.debug("returned without downloading because hits was None")
            return
        base_meta = [self._get_base_metadata_from_hit(h) for h in hits]
        base_metas = [self._guess_full_url(
            h, prefered_file_type, skip_not_prefered_extension) for h in base_meta]
        
        index_entries = [] # [[form_type, accn, file_name, file_num, filing_date], [...], ...]
        for m in base_metas:
            # check if file is in index, if so check if it actually exists,
            #  if both are true skip and log the skipped file and this reason
            file, save_name = self._download_filing(m["file_url"], m["skip"], m["fallback_url"])
            if resolve_urls and Path(save_name).suffix == ".htm":
                file = self._resolve_relative_urls(file, m["base_url"])
            if save is True:
                if file:
                    self._save_filing(m["cik"], m["form_type"], m["accession_number"], save_name, file, extract_zip=extract_zip)
                    if create_index is True:
                        file_nums = m["file_num"]
                        for file_num in file_nums:
                            index_entries.append([m["form_type"], m["accession_number"], save_name, file_num, m["filing_date"]])
                            # self.index_handler._create_indexes(m["cik"], m["form_type"], m["accession_number"], save_name, file_num, m["filing_date"])
                else:
                    logger.debug("didnt save/get filing despite that it should have. file was None")                
            if callback != None:
                callback({"file": file, "meta": m})
        try:
            self.index_handler._create_indexes_bulk(cik10, index_entries)
        except Exception as e:
            logger.debug(("undhandled exception in get_filings trying to create the index entries in bulk", e))
            raise e
        logger.info(f"Ticker: {self._current_ticker}, Downloads: {self._download_counter}, Form: {form_type}")           
        return
    
    def get_filings(
        self,
        ticker_or_cik: str,
        form_type: str,
        after_date: str = "",
        before_date: str = "",
        query: str = "",
        prefered_file_type: str = "",
        number_of_filings: int = 100,
        want_amendments: bool = True,
        skip_not_prefered_extension: bool = False,
        save: bool = True,
        extract_zip = True,
        create_index = True,
        resolve_urls: bool = True,
        callback = None):
        '''download filings.
        
        Args:
            ticker_or_cik: either a ticker symbol "AAPL" or a 10digit cik
            form_type: what form you want. valid forms are found in SUPPORTED_FILINGS
            after_date: date after which to consider filings
            before_date: date before which to consider filings
            query: query according to https://www.sec.gov/edgar/search/efts-faq.html.
            prefered_file_type: what filetype to prefer when looking for filings, see PREFERED_FILE_TYPES for handled extensions
            number_of_filings: how many filings to download.
            want_amendements: if we want to include amendment files or not
            skip_not_prefered_extension: either download or exclude if prefered_file_type
                               fails to match/download
            save: toggles saving the files downloaded.
            resolve_url: resolves relative urls to absolute ones in "htm"/"html" files 
            callback: pass a function that expects a dict of {"file": file, "meta": meta}
                      meta includes the metadata.
        '''
        # set these for info at end
        self._current_ticker = ticker_or_cik
        self._download_counter = 0

        cik10 = self._convert_to_cik10(ticker_or_cik)
        if prefered_file_type == (None or ""):
            if form_type not in PREFERED_FILE_TYPE_MAP.keys():
                logger.info(f"No Default file_type set for this form_type: {form_type}. defaulting to 'htm'")
                prefered_file_type = "htm"
            else:
                prefered_file_type = PREFERED_FILE_TYPE_MAP[form_type] 
                                
        logger.debug((f"\n Called get_filings with args: {locals()}"))
        hits = self._json_from_search_api(
            ticker_or_cik=cik10,
            form_type=form_type,
            number_of_filings=number_of_filings,
            want_amendments=want_amendments,
            after_date=after_date,
            before_date=before_date,
            query=query)
        if not hits:
            logger.debug("returned without downloading because hits was None")
            return
        base_meta = [self._get_base_metadata_from_hit(h) for h in hits]
        base_metas = [self._guess_full_url(
            h, prefered_file_type, skip_not_prefered_extension) for h in base_meta]
        
        for m in base_metas:
            # check if file is in index, if so check if it actually exists,
            #  if both are true skip and log the skipped file and this reason
            file, save_name = self._download_filing(m["file_url"], m["skip"], m["fallback_url"])
            if resolve_urls and Path(save_name).suffix == ".htm":
                file = self._resolve_relative_urls(file, m["base_url"])
            if save is True:
                if file:
                    self._save_filing(m["cik"], m["form_type"], m["accession_number"], save_name, file, extract_zip=extract_zip)
                    if create_index is True:
                        file_nums = m["file_num"]
                        for file_num in file_nums:
                            self.index_handler._create_indexes(m["cik"], m["form_type"], m["accession_number"], save_name, file_num, m["filing_date"])
                else:
                    logger.debug("didnt save/get filing despite that it should have. file was None")                
            if callback != None:
                callback({"file": file, "meta": m})
        logger.info(f"Ticker: {self._current_ticker}, Downloads: {self._download_counter}, Form: {form_type}")           
        return
    

    def get_xbrl_companyconcept(self, ticker_or_cik: str, taxonomy: str, tag: str):
        '''
        Args:
            ticker_or_cik: ticker like "AAPL" or cik like "1852973" or "0001852973"
            taxonomy: a taxonomy like "us-gaap" 
            tag: a concept tag like "AccountsPayableCurrent"
        Returns:
            python representation of the json file with contents described
            by the SEC as: "all the XBRL disclosures from a single company
            (CIK) and concept (a taxonomy and tag) [...] ,with a separate 
            array of facts for each units on measure that the company has
            chosen to disclose" 
            - https://www.sec.gov/edgar/sec-api-documentation
        '''
        cik10 = self._convert_to_cik10(ticker_or_cik)
        filename = tag + ".json"
        urlcik = "CIK"+cik10
        url = SEC_API_XBRL_COMPANYCONCEPT_URL
        for x in [urlcik, taxonomy, filename]:
            url = urljoin(url, x)
        resp = self._get(url=url, headers=self._sec_xbrl_api_headers)
        content = resp.json()
        return content
    
    def get_xbrl_companyfacts(self, ticker_or_cik: str) -> dict:
        '''download a companyfacts file.
        
        Args:
            ticker_or_cik: ticker like "AAPL" or cik like "1852973" or "0001852973"
        
        Returns:
            python representation of the json file with contents described
            by the SEC as: "all the company concepts data for a company"
            - https://www.sec.gov/edgar/sec-api-documentation
        '''     
        cik10 = self._convert_to_cik10(ticker_or_cik)
        # build URL
        filename = "CIK" + cik10 + ".json"
        url = urljoin(SEC_API_XBRL_COMPANYFACTS_URL, filename)
        # make call
        resp = self._get(url=url, headers=self._sec_xbrl_api_headers)
        content = resp.json()
        return content
    
    def get_bulk_companyfacts(self, extract=True):
        '''get all the companyfacts in one zip file (~1GB, extracted ~12GB)
        
        Args:
            extract: extract the zip into /companyfacts or just save the zip
                     in the root_path
        '''
        resp = self._get(url=SEC_BULK_COMPANYFACTS, headers=self._sec_files_headers)
        resp.raise_for_status()
        if resp:
            resp = resp.content
        if extract is True:
            (self.root_path / "companyfacts").mkdir(parents=True, exist_ok=True)
            save_path = self.root_path / "temp.zip"
            if save_path.exists() and save_path.is_file():
                save_path.unlink()
            save_path.write_bytes(resp)
            extract_path = self.root_path / "companyfacts"
            try:   
                with ZipFile(save_path, "r") as z:
                    z.extractall(extract_path)
            except BadZipFile as e:
                logger.info(f"zipfile info: {save_path.stat()}. \n somehow got a bad zipfile, make sure connection is stable")
            finally:    
                save_path.unlink()
        else:
            save_path = self.root_path / "companyfacts.zip"
            save_path.write_bytes(resp)
            
    
    def get_bulk_submissions(self, extract=True):
        '''get a file of all the sec submissions for every company in one zip file
            (~1.2GB, extracted ~6GB)
        
        Args:
            extract: extract the zip into /submissions or just save the zip
                     in the root_path
        '''
        resp = self._get(url=SEC_BULK_SUBMISSIONS, headers=self._sec_files_headers)
        resp.raise_for_status()
        if resp:
            resp = resp.content
        if extract is True:
            (self.root_path / "submissions").mkdir(parents=True, exist_ok=True)
            save_path = self.root_path / "temp.zip"
            if save_path.exists() and save_path.is_file():
                save_path.unlink()
            save_path.write_bytes(resp)
            extract_path = self.root_path / "submissions"   
            with ZipFile(save_path, "r") as z:
                z.extractall(extract_path)
            save_path.unlink()
        else:
            save_path = self.root_path / "submissions.zip"
            save_path.write_bytes(resp)


    def get_file_company_tickers(self) -> dict:
        '''download the cik, tickers file from the sec.
        
        The file structure: {index: {"cik_str": CIK, "ticker": ticker}}
        size: ~1MB
        '''
        resp = self._get(url=SEC_FILES_COMPANY_TICKERS, headers=self._sec_files_headers)
        content = resp.json()
        if "error" in content:
            logger.error("Couldnt fetch company_tickers.json file. got: {}", content)
        return content


    def get_file_company_tickers_exchange(self) -> dict:
        '''download the cik, ticker, exchange file from the sec
        
        The file structure: {"fields": [fields], "data": [[entry], [entry],...]}
        fields are: "cik", "name", "ticker", "exchange"
        size: ~600KB
        '''
        headers = { 
            "User-Agent": self.user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Host": "www.sec.gov"}
        resp = self._get(url=SEC_FILES_COMPANY_TICKERS_EXCHANGES, headers=headers)
        content = resp.json()
        if "error" in content:
            logger.error("Couldnt fetch company_ticker_exchange.json file. got: {}", content)
        return content

      
    def lookup_cik(self, ticker: str) -> str:
        '''look up the corresponding CIK for ticker and return it or an exception.

            Args:
                ticker: a symbol/ticker like: "AAPL"
            Raises:
                KeyError: ticker not present in lookuptable
        '''
        ticker = ticker.upper()
        cik = None
        try:
            cik = self._lookuptable_ticker_cik[ticker]
            return cik
        except KeyError as e:
            logger.error(f"{ticker} caused KeyError when looking up the CIK.")
            return e
        except Exception as e:
            logger.error(f"unhandled exception in lookup_cik: {e}")
            return e

    
    def set_session(self, session: requests.Session, sec_rate_limiting: bool = True):
        '''use a custom session object.

         Args:
            session: your instantiated session object
            sec_rate_limiting: toggle internal sec rate limiting,
                               Not advised to set False, 
                               can result in being locked out.
                                
        '''
        try:
            self._set_ratelimiting(sec_rate_limiting)
            if self._session:
                self._session.close()
            self._session = session
        except Exception as e:
            logger.error((
                f"Couldnt set new session, encountered {e}"
                f"Creating new default session"))
            self._create_session()
        return
    
    
    def _construct_sec_xbrl_api_headers(self):
        parsed = urlparse(SEC_API_XBRL_BASE)
        host = parsed.netloc
        return {
            "User-Agent": self.user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Host": host}
    
    def _construct_sec_files_headers(self):
        parsed = urlparse(SEC_FILES_COMPANY_TICKERS)
        host = parsed.netloc
        return {
            "User-Agent": self.user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Host": host}
    
    def _convert_to_cik10(self, ticker_or_cik: str):
        '''try to get the 10 digit cik from a ticker or a cik
        Args:
            ticker_or_cik: ticker like "AAPL" or cik like "1852973" or "0001852973"
        Returns:
            a 10 digit CIK as a string. ex: "0001841800"
        '''
        cik = None
        try:
            int(ticker_or_cik)
        except ValueError:
            #assume it is a ticker and not a cik
            cik = self.lookup_cik(ticker_or_cik)
        else:
            cik = ticker_or_cik
        if not isinstance(cik, str):
            cik = str(cik)
        cik10 = cik.zfill(10)
        return cik10

    def _set_ratelimiting(self, is_ratelimiting: bool):
        self._is_ratelimiting = is_ratelimiting
    
    def _load_or_update_lookuptable_ticker_cik(self) -> dict:
        '''load the tickers:cik lookup table and return it'''
        file = Path(TICKERS_CIK_FILE)
        if not file.exists():
            self._update_lookuptable_tickers_cik()
        with open(file, "r") as f:
            try:
                lookup_table = json.load(f)
                return lookup_table
            except IOError as e:        
                logger.error("couldnt load lookup table:  {}", e)
        return None
 
    def _update_lookuptable_tickers_cik(self):
        '''update or create the ticker:cik file'''
        content = self.get_file_company_tickers()
        if content:
            try:
                transformed_content = {}
                for d in content.values():
                    transformed_content[d["ticker"]] = d["cik_str"]
                if not Path(TICKERS_CIK_FILE).parent.exists():
                    Path(TICKERS_CIK_FILE).parent.mkdir(parents=True)
                with open(Path(TICKERS_CIK_FILE), "w") as f:
                    f.write(json.dumps(transformed_content))
            except Exception as e:

                logger.error((
                    f"couldnt update ticker_cik file."
                    f"unhandled exception: {e}"))
            # should add finally clause which restores file to inital state?
        else:
            raise ValueError("Didnt get content returned from get_file_company_tickers")
        return

    def _download_filing(self, file_url, skip, fallback_url=None):
        '''download a file and fallback on secondary url if 404. returns filing, save_name'''
        logger.debug((f"called _download_filing with args: {locals()}"))
        if file_url is None and skip is True:
            return None, None
        headers = self._sec_files_headers
        try:
            resp = self._get(url=file_url, headers=headers)
            # resp = self._get(url=base_meta["file_url"], headers=headers)
            resp.raise_for_status()
        except requests.HTTPError as e:
            if "404" in str(resp):
                if (skip is True) or (fallback_url is None):
                    # tried to get the prefered filetype but no file was found,
                    # so skip it according to "skip_not_prefered_extension"
                    logger.debug("skipping {}", locals())
                    return None, None
                resp = self._get(url=fallback_url, headers=headers)
                save_name = Path(fallback_url).name if isinstance(fallback_url, str) else fallback_url.name
        else:
            save_name = Path(file_url).name if isinstance(file_url, str) else file_url.name
        filing = resp.content if resp.content else None
        self._download_counter += 1
        return filing, save_name
    
    


        
    
    
    def _get_filing_save_path(self, ticker_or_cik, form_type, accn, file_name):
        # how to handle extracted zip files?
        return (self.root_path / "filings" / ticker_or_cik / form_type / accn / file_name)

    def _save_filing(self, cik, form_type, accession_number, save_name, file, extract_zip=False):
        # save by cik and add searching folders by ticker in query class
        '''save the filing and extract zips. '''
        save_path = self._get_filing_save_path(cik, form_type, accession_number, save_name)
        save_path.parent.mkdir(parents=True, exist_ok=True)
        save_path.write_bytes(file)
        logger.debug(f"saved file to: {save_path}")
        if (save_name[-3:] == "zip") and (extract_zip is True):
            with ZipFile(save_path, "r") as z:
                z.extractall(save_path.parent)
                logger.debug(f"extracted_zipfile successfully")
        return


    def _guess_full_url(self, base_meta, prefered_file_type, skip_not_prefered_extension):
        '''infers the filename of a filing and adds it and
        a fallback to the base_meta dict. returns the changed base_meta dict
        '''
        # rethink this whole skip_not_prefered_extension thing -naming -usefulness -implementation
        base_url = base_meta["base_url"]
        accession_number = base_meta["accession_number"]
        base_meta["fallback_url"] = urljoin(base_url, base_meta["main_file_name"])
        # assume that main_file is the one relevant unless specified (eg: xbrl)
        
        suffix = Path(base_meta["main_file_name"]).suffix.replace(".", "")
        logger.debug(f"suffix of file: {suffix}, prefered_file_type: {prefered_file_type}")
        if suffix == prefered_file_type:
            base_meta["file_url"] = urljoin(base_url, base_meta["main_file_name"])
        else:
            if prefered_file_type == "xbrl":
                # only add link to the zip file for xbrl to reduce amount of requests
                base_meta["file_url"] = urljoin(base_url, (accession_number + "-xbrl.zip"))
            elif prefered_file_type == "txt":
                # get full text submission
                base_meta["file_url"] = urljoin(base_url, base_meta["main_file_name"].replace(suffix, "txt"))
            elif suffix == ("html" or "htm") and prefered_file_type == ("html" or "htm"):
                # html is htm therefor treat them as equal
                if suffix == "html":
                    base_meta["file_url"] = urljoin(base_url, base_meta["main_file_name"].replace(suffix, "htm"))
                else:
                    base_meta["file_url"] = urljoin(base_url, base_meta["main_file_name"])
            # xml is implicitly covered
            elif not skip_not_prefered_extension:
                base_meta["file_url"] = urljoin(base_url, base_meta["main_file_name"])
        if skip_not_prefered_extension and "file_url" not in base_meta.keys():
            base_meta["file_url"] = None
            skip = True
        else:
            skip = False 
        base_meta["skip"] = skip
        logger.debug(
            (f"guessing for main_file_name: {base_meta['main_file_name']} \n"
             f"with prefered_file_type: {prefered_file_type} and \n"
             f"skip_not_prefered_extension: {skip_not_prefered_extension} \n"
             f"created file_url: {base_meta['file_url']} \n"
             f"created fallback_url:{base_meta['fallback_url']}\n"))
        return base_meta
    
    def _resolve_relative_urls(self, filing: str, base_url: str):
        # soup content then resolve relative links and image locations
        soup = BeautifulSoup(filing)
        base = base_url
        for rurl in soup.find_all("a", href=True):
            href = rurl["href"]
            if href.startswith("http") or href.startswith("#"):
                pass
            else:
                rurl["href"] = urljoin(base, href)
        
        for image in soup.find_all("img", src=True):
            image["src"] = urljoin(base, image["src"])
        
        return soup.encode(soup.original_encoding) if soup.original_encoding else soup
    

    def _get_systime_ms(self):
        return int(time.time() * SEC_RATE_LIMIT_DELAY)
    

    def _get_base_metadata_from_hit(self, hit: dict):
        '''getting the most relevant information out of a entry. returning a dict'''
        accession_number, filing_details_filename = hit["_id"].split(":", 1)
        accession_number_no_dash = accession_number.replace("-", "", 2)
        cik = hit["_source"]["ciks"][-1]
        film_num = hit["_source"]["film_num"]
        submission_base_url = urljoin(urljoin(EDGAR_ARCHIVES_BASE_URL, cik),(accession_number_no_dash))
        xsl = hit["_source"]["xsl"] if hit["_source"]["xsl"] else None
        filing_date = hit["_source"]["file_date"]
        return {
            "form_type": hit["_source"]["root_form"],
            "accession_number": accession_number,
            "cik": cik,
            "base_url": submission_base_url,
            "main_file_name": filing_details_filename,
            "xsl": xsl,
            "file_num": film_num,
            "filing_date": filing_date}
   

    def _json_from_search_api(
            self,
            ticker_or_cik: str,
            form_type: str,
            number_of_filings: int = 20,
            want_amendments = False,
            after_date: str = "",
            before_date: str = "",
            query: str = ""
            ) -> dict:
        '''gets a list of filings submitted to the sec.'''
        gathered_responses = []
        headers = { 
            "User-Agent": self.user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Host": "efts.sec.gov"}
        start_index = 0
        while len(gathered_responses) < number_of_filings:
            post_body = {
                "dateRange": "custom",
                "startdt": after_date,
                "enddt": before_date,
                "entityName": ticker_or_cik,
                "forms": [form_type],
                "from": start_index,
                "q": query}
            resp = self._post(url=SEC_SEARCH_API_URL, json=post_body, headers=headers)
            resp.raise_for_status()
            result = resp.json()

            logger.debug(f"result from POST call: {result}")
            
            if "error" in result:
                try:
                    root_cause = result["error"]["root_cause"]
                    if not root_cause:
                        raise ValueError
                    else:
                        raise ValueError(f"error reason: {root_cause[0]['reason']}")  
                except (KeyError, IndexError) as e:
                    raise e
            if not result:
                break

            if result["hits"]["hits"] == []:
                if len(gathered_responses) == 0:
                    logger.info(f"[{ticker_or_cik}:{form_type}] -> No filings found for this combination")
                break
            
            for res in result["hits"]["hits"]:
                # only filter for amendments here
                res_form_type = res["_source"]["file_type"]
                is_amendment = res_form_type[-2:] == "/A"
                if not want_amendments and is_amendment:
                    continue
                # make sure that no wrong filing type is added
                if (not is_amendment) and (res_form_type != form_type):
                    continue
                # make sure to only get filings after date and before date
                # assuming that all entries are ordered descending by time
                if (after_date != "") and (res["_source"]["file_date"] < after_date):
                    break
                if (before_date != "") and (res["_source"]["file_date"] > before_date):
                    break
                gathered_responses.append(res)

            query_size = result["query"]["size"]
            start_index += query_size
        return gathered_responses[:number_of_filings]
    
    def _rate_limit(func):
        '''decorate a function to limit call rate in a synchronous program.
        Can be toggled on/off by calling set_ratelimiting(bool)'''
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if self._is_ratelimiting is False:
                return func(self, *args, **kwargs)
            time.sleep(
                max(0, ((self._next_try_systime_ms - self._get_systime_ms()
                ) / 1000) ))
            result = func(self, *args, **kwargs)
            self._next_try_systime_ms = self._get_systime_ms(
            ) + SEC_RATE_LIMIT_DELAY 
            return result
        return wrapper
        
    @_rate_limit
    def _get(self, *args, **kwargs):
        '''wrapped to comply with sec rate limit across calls'''
        return self._session.get(*args, **kwargs)


    @_rate_limit
    def _post(self, *args, **kwargs):
        '''wrapped to comply with sec rate limit across calls'''
        return self._session.post(*args, **kwargs)
        

    def _create_session(self, retry=10) -> requests.Session:
        '''create a session used by the Downloader with a retry
        strategy on all urls. retries on status:
            500, 502, 503, 504, 403 .'''
        r = Retry(
            total=retry,
            read=retry,
            connect=retry,
            backoff_factor = float(0.3),
            status_forcelist=(500, 502, 503, 504, 403))
        r.backoff_max = 10
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=r) 
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

