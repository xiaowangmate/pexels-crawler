import os
import re
import json
import time
import cfscrape

scraper = cfscrape.create_scraper()


class PexelsKeywordCrawler:
    def __init__(self, keywords_path, output_base_dir):
        self.headers = {
            "Referer": "https://www.pexels.com/videos/",
            "Secret-Key": "H2jk9uKnhRmL6WPwh89zBezWvr",
            "Cookie": '_ga=GA1.1.1897440495.1712042147; _fbp=fb.1.1712042152434.1774328176; country-code-v2=HK; g_state={"i_p":1712137685143,"i_l":1}; _sp_ses.9ec1=*; __cf_bm=ZR8kyxZECGmX1rZ3CNNWKoQR98ISO.BoM1dae5TzPQ4-1712137983-1.0.1.1-NJRbYLOhMWka0QHKu736aOjl8lctiVZ99a0N0dZtMwVt3hV5zznpY7JRAZIG61ZKfBuR.Q0XVKlFLu0T.FjyOQ; _sp_id.9ec1=cced12ff-e652-489f-bbab-51b9b8cf9a3d.1712042147.7.1712137986.1712133851.92c8e04a-fd66-48f8-a083-a71124ad6dee.00145d2b-afa1-4b1a-a3d7-a131d64139fe.bd54ed3a-69a0-4423-827a-cf8c7ab22884.1712137981625.3; cf_clearance=PWZ.nkDvFTEjyzRQRc4xR9ql4ZMlbOz5MZB83rtteHc-1712137985-1.0.1.1-ypVupUy_0LkFqP0WB0P7eIHKM_iU6FKtfMI0bCwNDCbI_8rcauy7T531GWbPhwVtKT93kACPMGT2wYrvswAlFg; OptanonConsent=isGpcEnabled=0&datestamp=Wed+Apr+03+2024+17%3A53%3A06+GMT%2B0800+(%E4%B8%AD%E5%9B%BD%E6%A0%87%E5%87%86%E6%97%B6%E9%97%B4)&version=202301.1.0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A0%2CC0003%3A0%2CC0004%3A0&AwaitingReconsent=false; _ga_8JE65Q40S6=GS1.1.1712137982.7.1.1712137986.0.0.0',
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36 Edg/118.0.2088.46"
        }
        self.search_url = "https://www.pexels.com/en-us/api/v3/search/videos?page={}&per_page=24&query={}&orientation=all&size=all&color=all&sort=popular&seo_tags=true"
        self.keywords_path = keywords_path
        self.current_processing_keyword_index_path = "current_processing_keyword_index.txt"
        self.crawled_ids_path = "crawled_id_list.txt"
        self.output_base_dir = output_base_dir
        self.dir_check(self.output_base_dir)
        self.file_check(self.current_processing_keyword_index_path, "0")
        self.file_check(self.crawled_ids_path, "")
        self.crawled_id_list = self.read_crawled_ids()
        self.keyword_index = int(self.read_current_processing_keyword_index())
        self.keyword = ""

    def read_crawled_ids(self):
        with open(self.crawled_ids_path, mode="r", encoding="utf-8") as ids:
            return ids.read().split("\n")

    def read_current_processing_keyword_index(self):
        with open(self.current_processing_keyword_index_path, mode="r", encoding="utf-8") as index:
            return index.read()

    def dir_check(self, target_dir_path):
        if not os.path.exists(target_dir_path):
            os.mkdir(target_dir_path)

    def file_check(self, target_file_path, content):
        if not os.path.exists(target_file_path):
            with open(target_file_path, mode="w", encoding="utf-8") as file:
                file.write(content)

    def update_crawled_ids(self, video_id):
        with open(self.crawled_ids_path, mode="a+", encoding="utf-8") as ids:
            ids.write(f"{video_id}\n")
            self.crawled_id_list.append(video_id)

    def update_keyword_index(self, keyword_index):
        with open(self.current_processing_keyword_index_path, mode="w", encoding="utf-8") as ki:
            ki.write(f"{keyword_index}")

    def video_info_record(self, video_id, caption):
        with open(f"{self.output_base_dir}/video_info.jsonl", mode="a+", encoding="utf-8") as record:
            video_info_dict = {"video_id": video_id, "keyword": self.keyword, "caption": caption}
            record.write(f"{json.dumps(video_info_dict)}\n")
            print(f"record video info: {video_info_dict}")

    def start_crawl(self):
        with open(self.keywords_path, mode="r", encoding="utf-8") as r:
            keywords = r.read().strip().split("\n")
            for keyword in keywords[self.keyword_index:]:
                self.keyword = keyword.strip()
                self.dir_check(f"{self.output_base_dir}/{self.keyword}")
                index_url = self.search_url.format(1, self.keyword)
                response = json.loads(scraper.get(index_url, headers=self.headers).text)
                data = response["data"]
                self.parse(data)

                total_pages = self.get_total_pages(response)
                for i in range(2, int(total_pages) + 1):
                    page_url = self.search_url.format(i, self.keyword)
                    response = json.loads(scraper.get(page_url, headers=self.headers).text)
                    data = response["data"]
                    self.parse(data)

                self.keyword_index += 1
                self.update_keyword_index(self.keyword_index)

    def parse(self, data):
        for video in data:
            video_id = video["id"]
            # if video_id not in self.crawled_id_list:
            if video["attributes"]["width"] > video["attributes"]["height"]:
                self.select_resolution_for_download(video)
            else:
                self.select_resolution_for_download(video, resolution="720x1280")
            # else:
            #     print(f"Video exist: {video_id}")

    def get_total_pages(self, response):
        total_pages = response["pagination"]["total_pages"]
        return total_pages

    def download_url_matching(self, selected_resolution_download_url, download_url_list):
        best_match_url = None
        best_match_diff = float("inf")
        selected_resolution_width, selected_resolution_height = map(int, re.findall(r'(\d+)_', selected_resolution_download_url))
        for download_url in download_url_list:
            link = download_url["link"]
            if selected_resolution_download_url == link:
                return link
            else:
                width, height = map(int, re.findall(r'(\d+)_', link))
                diff = abs(selected_resolution_width - width) + abs(selected_resolution_height - height)
                if diff < best_match_diff:
                    best_match_url = link
                    best_match_diff = diff
        return best_match_url

    def select_resolution_for_download(self, video_json, resolution="1280x720"):
        """
        default resolution list(width x height): [426x240, 640x360, 960x540, 1280x720, 1920x1080, 2560x1440, 3840x2160]
        """
        width, height = resolution.split("x")
        if (int(width) >= 2560 and int(height) >= 1440) or (int(width) >= 1440 and int(height) >= 2560):
            quality = "uhd"
        elif (int(width) >= 1280 and int(height) >= 720) or (int(width) >= 720 and int(height) >= 1280):
            quality = "hd"
        else:
            quality = "sd"
        resolution = resolution.replace('x', '_')
        video_id = video_json["id"]
        output_path = f"{self.output_base_dir}/{self.keyword}/{video_id}.mp4"
        if not os.path.exists(output_path):
            default_download_url = video_json["attributes"]["video"]["src"]
            print(f"default_download_url: {default_download_url}")
            if default_download_url:
                print(f'video_json["attributes"]: {video_json["attributes"]}')
                fps = \
                    re.findall("https://videos.pexels.com/video-files/.*?/.*?_(\d{1,3})fps.mp4", default_download_url)[
                        0]
                selected_resolution_download_url = f"https://videos.pexels.com/video-files/{video_id}/{video_id}-{quality}_{resolution}_{fps}fps.mp4"
                download_url = self.download_url_matching(selected_resolution_download_url, video_json["attributes"]["video"]["video_files"])
                self.download_video(video_id, download_url, output_path)
                if video_id not in self.crawled_id_list:
                    caption = video_json["attributes"]["title"]
                    self.video_info_record(video_id, caption)
            else:
                print(f"Video error: {video_id}, {video_json}")
        else:
            print(f"Video exist: {video_id}")
        self.update_crawled_ids(video_id)
        print("-" * 100)

    def download_video(self, video_id, download_url, output_path):
        response = scraper.get(download_url, stream=True, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36 Edg/118.0.2088.46"})
        if response.status_code == 200:
            with open(output_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        file.write(chunk)
            print(f"Video downloaded successfully: {video_id}")
            time.sleep(0.1)
        else:
            print(f"Failed to download video: {download_url}, {response.text}")


if __name__ == '__main__':
    pkc = PexelsKeywordCrawler("humankeywords.txt", "G:\Pexels Human")
    pkc.start_crawl()
