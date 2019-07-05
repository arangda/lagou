import requests
import re
import time
from requests.packages.urllib3.exceptions import InsecureRequestWarning
# 禁用安全请求警告
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class HandleLaGou(object):
    def __init__(self):
        #使用session保存cookies信息
        self.lagou_session = requests.session()
        self.header = {
            'User-Agent': 'Mozilla / 5.0(Windows NT 6.1;Win64;x64) AppleWebKit / 537.36(KHTML, likeGecko) Chrome / 75.0.3770.100Safari / 537.36'
        }
        self.city_list = ""

    #获取全国所有城市列表的方法
    def handle_city(self):
        city_search = re.compile(r'zhaopin/">(.*?)</a>')
        city_url = 'https://www.lagou.com/jobs/allCity.html'
        city_result = self.handle_request(method='GET', url=city_url)
        self.city_list = city_search.findall(city_result)

    def handle_city_job(self,city):
        first_request_url = "https://www.lagou.com/jobs/list_python?px=default&city=%s"%city
        first_response = self.handle_request(method="GET", url=first_request_url)
        total_page_search = re.compile(r'class="span\stotalNum">(\d+)</span>')
        try:
            total_page = total_page_search.search(first_response).group(1)
        #没有岗位信息造成的exception
        except:
            return
        else:
            for i in range(1,int(total_page)+1):
                data = {
                    "pn": i,
                    "kd": "python"
                }
                page_url = "https://www.lagou.com/jobs/positionAjax.json?px=default&city=%s&needAddtionalResult=false"%city
                referer_url = "https://www.lagou.com/jobs/list_python?px=default&city=%s"%city
                #referer的URL需要进行encode
                self.header['Referer'] = referer_url.encode()
                response = self.handle_request(method="POST",url=page_url,data=data,info=city)
                print(response)

    def handle_request(self, method, url, data=None, info=None):
        while True:
            if method == "GET":
                response = self.lagou_session.get(url, headers=self.header, verify=False)
            elif method == "POST":
                response = self.lagou_session.post(url,headers=self.header,data=data, verify=False)
            response.encoding = 'utf-8'

            if '频繁' in response.text:
                #需要先清除cookies信息
                self.lagou_session.cookies.clear()
                #重新获取cookies
                first_request_url = "https://www.lagou.com/jobs/list_python?px=default&city=%s" %info
                self.handle_request(method="GET", url=first_request_url)
                time.sleep(10)
                continue
            return response.text

if __name__ == '__main__':
    lagou = HandleLaGou()
    lagou.handle_city()
    lagou.lagou_session.cookies.clear()
    lagou.handle_city_job('北京')

