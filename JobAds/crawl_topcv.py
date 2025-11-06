# from typing import override
from bs4 import BeautifulSoup
from bs4.element import Tag
from datetime import date, datetime, timedelta
from time import sleep
import pandas as pd # Cần thư viện này để dễ dàng xuất ra CSV
import json
from datetime import datetime

# Import giả định cho các dependencies bên ngoài
# Bạn cần đảm bảo các dependency này đã được cài đặt và hàm send_request hoạt động.
# Ví dụ đơn giản cho send_request (cần thay thế bằng implementation thực tế):
import requests
def send_request(method: str, url: str):
    if method.lower() == "get":
        # Thêm headers để giả lập trình duyệt có thể cần thiết
        return requests.get(url, timeout=10) 
    raise NotImplementedError(f"Method {method} not implemented")

USD_TO_VND = 24000

#################################################


class PageProcessor():
    """
    Processor class for job listing pages.
    """
    def generate_page_urls(self, 
        url: str, 
        recursive: bool = False
    ):
        """
        Generate job detail page URLs to JobProcessor and its derived classes.

        Arguments:
            url [str]: Processed URL.
            recursive [bool]: Enable processing pages recursively.

        Yields:
            job_url [str]: URL for job detail pages.

        Usage: 
            detail_url_gen = PageProcessor().get_job_detail_urls(
                <job_listing_url>,
                ...
            )
            for job_url in detail_url_gen:
                do something 
        """
        # Send requests and parse job details page and gather job data
        print("Scraping job URLs at", url)
        try:
            response = send_request("get", url)
        except requests.exceptions.RequestException as e:
            print(f"Error requesting {url}: {e}")
            return
            
        soup = BeautifulSoup(response.content, "html.parser")
        jobs_in_page = soup.find_all("div", "job-item-2")
        for job in jobs_in_page:
            # Dùng .get("href") để tránh KeyError nếu 'href' không tồn tại
            link_tag = job.find("a", target = "_blank")
            if link_tag and link_tag.get("href"):
                job_url = link_tag["href"]
                yield job_url
            else:
                print("Warning: Job item found without a valid link.")

        # Process next page if found
        next_page_tag = soup.find("a", rel = "next")
        if next_page_tag and next_page_tag.get("href"):
            next_page_url = next_page_tag["href"]
            if next_page_url and recursive:
                print("Page finished. Moving on to next page.")
                for job_url in self.generate_page_urls(next_page_url, recursive):
                    yield job_url
        print("Page finished. Crawl ended.")


#################################################


class JobProcessor():
    """
    Base class for processors of job details page from given URL. 
    Main entrypoint for job page processing applications. 
    Can handle different site templates.
    """
    # Khởi tạo cơ bản cho lớp cha
    def __init__(self):
        pass

    def process_job(self, url: str, pause_between_jobs: int = 3):
        """
        Parse URL to find job detail page template based on first-level subdirectory

        Arguments:
            url [str]: URL of job detail page.
            pause_between_jobs [int]: Seconds between each request 
                to get job detail page.

        Returns:
            job_item [dict]: Processed data.
        
        Usage: To scrape a job detail page onto job_item:
            job_item = Processor().process_job(<job_detail_url>)
        """
        print(f"Scraping job info at {url}...")

        # Parse URL and get keyword
        # Xử lý trường hợp URL có thể không bắt đầu bằng "https://www." hoặc "http://"
        try:
            # Loại bỏ scheme (http/https) và domain, lấy phần path
            path = url.split("://", 1)[-1].split("/", 1)[-1]
            keyword = path.split("/")[0]
        except IndexError:
            raise ValueError(f"URL format is not recognized: {url}")

        keyword_map = {
            "viec-lam": _NormalJobProcessor(),
            "brand": _BrandJobProcessor()
        }

        # Instantiate suitable processor based on URL keyword
        try:
            processor = keyword_map[keyword]
        except KeyError:
            raise ValueError(f"Strange URL syntax detected (keyword: {keyword}). \
Wrong URL input or parsing for this page has not been implemented.")
        
        # Process job based on newly assigned processor
        sleep(pause_between_jobs)
        return processor._process_job_details(url) # Đổi tên thành _process_job_details để tránh lặp tên


    def _process_salary(self, salary_tag: Tag):
        """
        Parse salary tag into integer salary range (in million VND).
        Detects USD and convert to million VND.
        """
        salary_str = salary_tag.text.strip() # Dùng strip() đơn giản hơn

        if salary_str == "Thoả thuận":       # Default string for no salary info
            return None, None
        
        # Remove , thousand separators
        # Dùng regex hoặc list comprehension để đảm bảo lấy được các giá trị số
        import re
        salary_arr = re.split(r'[\s\u200b]+', salary_str) # Tách bằng khoảng trắng và zero-width space
        salary_values = []
        unit = "VND" # Giả định mặc định là VND

        for item in salary_arr:
            item = item.replace(",", "")
            if item.isdigit():
                salary_values.append(int(item))
            elif item.upper() == "USD":
                unit = "USD"
        
        min_salary, max_salary = None, None
        
        if len(salary_values) == 2:          # Normal range (<min> - <max> <unit>)
            min_salary, max_salary = salary_values[0], salary_values[1]
        elif salary_str.startswith("Trên") and len(salary_values) == 1: # Min only (Trên <min> <unit>)
            min_salary = salary_values[0]
        elif salary_str.startswith("Tới") and len(salary_values) == 1: # Max only (Tới <max> <unit>)
            max_salary = salary_values[0]
        elif len(salary_values) == 1:        # Chỉ có 1 con số
            min_salary, max_salary = salary_values[0], salary_values[0]

        # Convert USD to million VND
        if unit == "USD":
            min_salary = int(min_salary * USD_TO_VND / 10**6) if min_salary is not None else None
            max_salary = int(max_salary * USD_TO_VND / 10**6) if max_salary is not None else None
        else: # Convert VND to million VND (VND là đơn vị, thường đã là nghìn/triệu)
              # Giả định nếu không phải USD thì đơn vị trên trang là triệu VND
            min_salary = min_salary if min_salary is not None else None
            max_salary = max_salary if max_salary is not None else None


        return min_salary, max_salary
    
    def _process_xp(self, xp_tag: Tag):
        # Returns min & max required experience (years)
        xp_str = xp_tag.text.strip()
        xp_arr = xp_str.split(" ")

        if xp_str == "Không yêu cầu kinh nghiệm":
            return 0, 0
        
        # Trích xuất số năm kinh nghiệm
        xp_num = None
        for item in xp_arr:
            if item.isnumeric():
                xp_num = int(item)
                break

        if xp_num is None:
            return None, None
        
        if xp_arr[0].isnumeric(): # <xp> năm
            return xp_num, xp_num
        elif xp_arr[0] == "Trên": # Trên <xp> năm
            return xp_num, None
        elif xp_arr[0] == "Dưới": # Dưới <xp> năm
            return None, xp_num
        else:
            return None, None


class _NormalJobProcessor(JobProcessor):
    """
    Used for processing job detail pages with ./viec-lam/... subdirectories
    """
    def __init__(self):
        super().__init__()
        
    # @override
    def _process_job_details(self, url: str):
        # Send request, instantiate BS object and define necessary tags
        response = send_request("get", url)
        soup = BeautifulSoup(response.content, "html.parser")
        detail_tags = soup.find_all("div", 
            class_ = "job-detail__info--section-content-value"
        )   # [salary_tag, city_tag, yrs_of_exp_tag]
        
        title_tag = soup.find("h1", class_ = "job-detail__info--title")
        company_tag = soup \
            .find("h2", class_ = "company-name-label")
        company_link = company_tag.find("a") if company_tag else None
        
        salary_tag = detail_tags[0] if len(detail_tags) > 0 else None
        xp_tag = detail_tags[2] if len(detail_tags) > 2 else None
        city_tag = detail_tags[1] if len(detail_tags) > 1 else None
        due_tag = soup.find("div", class_ = "job-detail__info--deadline")
        jd_tag = soup.find("div", class_ = "job-description__item--content")

        # Process field values
        job_id = int(url.split("/")[-1].split(".")[0])
        job_title = title_tag.text.strip() if title_tag else "N/A"
        company = company_link.text.strip() if company_link else "N/A"
        
        salary_min, salary_max = self._process_salary(salary_tag) if salary_tag else (None, None)
        yrs_of_exp_min, yrs_of_exp_max = self._process_xp(xp_tag) if xp_tag else (None, None)
        job_city = city_tag.text.strip() if city_tag else "N/A"
        
        due_date = None
        if due_tag:
            date_str = due_tag.text.split(" ")[-1].strip()
            try:
                due_date = datetime.strptime(date_str, "%d/%m/%Y")
            except ValueError:
                pass # Để due_date là None nếu parsing thất bại

        jd = jd_tag.text.strip() if jd_tag else "N/A"

        return {
            "job_id": job_id,
            "job_title": job_title,
            "company": company,
            "salary_min": salary_min,
            "salary_max": salary_max,
            "yrs_of_exp_min": yrs_of_exp_min,
            "yrs_of_exp_max": yrs_of_exp_max,
            "job_city": job_city,
            "due_date": due_date,
            "jd": jd
        }


class _BrandJobProcessor(JobProcessor):
    """
    Used for processing job detail pages with ./brand/... subdirectories.
    """
    def __init__(self):
        super().__init__()
        
    # @override
    def _process_job_details(self, url: str):
        # Send request, instantiate BS object
        response = send_request("get", url)
        soup = BeautifulSoup(response.content, "html.parser")
        
        # Recognize template and select appropriate strategy
        if soup.find("div", id = "premium-job"):
            job_item = self._process_job_premium(soup, url)
        else:
            job_item = self._process_job_diamond(soup, url)
        return job_item

    def _process_job_diamond(self, soup: BeautifulSoup, url: str):
        # Define necessary tags
        box_infos = soup.find_all("div", class_ = "box-info", limit = 2)
        
        item_tags = None
        if box_infos and len(box_infos) > 0:
             box_main = box_infos[0].find("div", class_ = "box-main")
             if box_main:
                 item_tags = box_main.find_all("div", class_ = "box-item")

        title_tag = soup \
            .find("div", class_ = "box-header") \
            .find("h2", class_ = "title") if soup.find("div", class_ = "box-header") else None
            
        company_tag = soup.find("div", class_ = "footer-info-company-name")
        salary_tag = item_tags[0].find("span") if item_tags and len(item_tags) > 0 else None
        xp_tag = item_tags[-1].find("span") if item_tags and len(item_tags) > 0 else None
        
        city_tag_parent = soup.find("div", class_ = "box-address")
        city_tag = city_tag_parent.find("div") if city_tag_parent else None # Type 1 syntax
        
        due_tag = soup.find("span", class_ = "deadline")
        due_tag_strong = due_tag.find("strong") if due_tag else None
        
        jd_tag = box_infos[1].find("div", class_ = "content-tab") if len(box_infos) > 1 else None

        # Get job detail values
        try:
             # Lấy id từ URL: <url>/brand/abc-p<id>.html
            job_id = int(url.split("/")[-1].split(".")[0].split("-")[-1][1:])
        except:
            job_id = None
            
        job_title = title_tag.text.strip() if title_tag else "N/A"
        company = company_tag.text.strip() if company_tag else "N/A"
        
        salary_min, salary_max = self._process_salary(salary_tag) if salary_tag else (None, None)
        yrs_of_exp_min, yrs_of_exp_max = self._process_xp(xp_tag) if xp_tag else (None, None)
        
        job_city = "N/A"
        if city_tag and ":" in city_tag.text:
            # Cắt chuỗi: "[Địa điểm:] Tên thành phố" -> Tên thành phố
            job_city = city_tag.text.split(":")[-1].strip()

        due_date = None
        if due_tag_strong:
            try:
                days_remaining = int(due_tag_strong.text)
                # Đảm bảo due_date là datetime.datetime (giống các processor khác)
                due_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days = days_remaining)
            except ValueError:
                pass # Để due_date là None

        jd = jd_tag.text.strip() if jd_tag else "N/A"
        
        return {
            "job_id": job_id,
            "job_title": job_title,
            "company": company,
            "salary_min": salary_min,
            "salary_max": salary_max,
            "yrs_of_exp_min": yrs_of_exp_min,
            "yrs_of_exp_max": yrs_of_exp_max,
            "job_city": job_city,
            "due_date": due_date,
            "jd": jd
        }

    def _process_job_premium(self, soup: BeautifulSoup, url: str):
        # Define necessary tags
        detail_tags = soup.find_all(
            "div", 
            class_ = "basic-information-item__data--value"
        )

        title_tag = soup.find("h2", "premium-job-basic-information__content--title")
        company_tag = soup.find("h1", "company-content__title--name")
        salary_tag = detail_tags[0] if len(detail_tags) > 0 else None
        xp_tag = detail_tags[-1] if len(detail_tags) > 0 else None
        city_tag = detail_tags[1] if len(detail_tags) > 1 else None
        
        due_tag_all = soup.find_all("div", class_ = "general-information-data__value")
        due_tag = due_tag_all[-1] if due_tag_all else None
        
        jd_tag = soup.find("div", class_ = "premium-job-description__box--content")

        # Get job detail values
        try:
            # Lấy id từ URL: <url>/brand/abc-p<id>.html
            job_id = int(url.split("/")[-1].split(".")[0].split("-")[-1][1:])
        except:
            job_id = None
            
        job_title = title_tag.text.strip() if title_tag else "N/A"
        company = company_tag.text.strip() if company_tag else "N/A"
        
        salary_min, salary_max = self._process_salary(salary_tag) if salary_tag else (None, None)
        yrs_of_exp_min, yrs_of_exp_max = self._process_xp(xp_tag) if xp_tag else (None, None)
        job_city = city_tag.text.strip() if city_tag else "N/A"
        
        due_date = None
        if due_tag:
            date_str = due_tag.text.split(" ")[-1].strip()
            try:
                due_date = datetime.strptime(date_str, "%d/%m/%Y")
            except ValueError:
                pass

        jd = jd_tag.text.strip() if jd_tag else "N/A"
        
        return {
            "job_id": job_id,
            "job_title": job_title,
            "company": company,
            "salary_min": salary_min,
            "salary_max": salary_max,
            "yrs_of_exp_min": yrs_of_exp_min,
            "yrs_of_exp_max": yrs_of_exp_max,
            "job_city": job_city,
            "due_date": due_date,
            "jd": jd
        }

# --- Các hàm mới để lưu file ---

def save_to_csv(data: list[dict], filename: str):
    """Lưu danh sách dictionaries ra tệp CSV."""
    try:
        # Chuyển danh sách dicts sang DataFrame
        df = pd.DataFrame(data)
        
        # Chuyển đổi đối tượng datetime/date sang chuỗi định dạng
        for col in df.select_dtypes(include=['datetime64[ns]']).columns:
            df[col] = df[col].dt.strftime('%Y-%m-%d')
            
        df.to_csv(filename, index=False, encoding='utf-8-sig') # Dùng utf-8-sig để hỗ trợ tiếng Việt tốt hơn
        print(f"✅ Data successfully saved to **{filename}** (CSV).")
    except Exception as e:
        print(f"❌ Error saving data to CSV: {e}")

def save_to_json(data: list[dict], filename: str):
    """Lưu danh sách dictionaries ra tệp JSON."""
    try:
        # Chuẩn bị dữ liệu: Chuyển đổi các đối tượng datetime thành chuỗi
        serializable_data = []
        for item in data:
            new_item = {}
            for k, v in item.items():
                if isinstance(v, datetime):
                    new_item[k] = v.strftime('%Y-%m-%d')
                else:
                    new_item[k] = v
            serializable_data.append(new_item)
            
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(serializable_data, f, ensure_ascii=False, indent=4)
        print(f"✅ Data successfully saved to **{filename}** (JSON).")
    except Exception as e:
        print(f"❌ Error saving data to JSON: {e}")

# --- Điểm khởi đầu để chạy toàn bộ chương trình ---

def main():
    """
    Main function to orchestrate the job scraping process.
    """
    # Thay thế bằng URL trang danh sách công việc thực tế của bạn
    start_url = "https://www.topcv.vn/viec-lam-it?page=2" 
    
    # Khởi tạo các processor
    page_processor = PageProcessor()
    job_processor = JobProcessor()
    
    crawled_jobs = []
    
    print("--- Starting Job Crawler ---")
    
    # Bắt đầu tạo URL chi tiết
    # Đặt recursive=True nếu bạn muốn crawl tất cả các trang
    detail_url_generator = page_processor.generate_page_urls(start_url, recursive=False)  # recursive=False
    
    for job_url in detail_url_generator:
        try:
            # Xử lý từng URL chi tiết với khoảng nghỉ 1 giây giữa các job
            job_data = job_processor.process_job(job_url, pause_between_jobs=1)
            crawled_jobs.append(job_data)
            print(f"Successfully scraped: {job_data['job_title']} at {job_data['company']}")
        except (ValueError, requests.exceptions.RequestException, Exception) as e:
            print(f"Failed to process job URL {job_url}: {e}")
            
    print(f"\n--- Crawling Finished ---")
    print(f"Total jobs crawled: {len(crawled_jobs)}")
    # print(crawled_jobs) # In ra kết quả nếu cần
    # --- PHẦN MỚI: LƯU DỮ LIỆU ---
    
    if crawled_jobs:
        # Lấy timestamp để đặt tên file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 1. Lưu dưới dạng CSV
        save_to_csv(crawled_jobs, f"job_data_{timestamp}.csv")
        
        # 2. Lưu dưới dạng JSON
        save_to_json(crawled_jobs, f"job_data_{timestamp}.json")
    else:
        print("No data was crawled to save.")

# Chỉ chạy main khi script được thực thi trực tiếp
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"An unexpected error occurred in main execution: {e}")