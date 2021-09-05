# DBUpdater.py
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd
import urllib, pymysql, calendar, time, json
from urllib.request import urlopen
from datetime import datetime
from threading import Timer
import requests

class DBUpdater:
    def __init__(self):
        """생성자: MariaDB 연결 및 종목코드 딕셔너리 생성"""
        # DB Connection 
        self.conn = pymysql.connect(host='192.168.219.105', user='leefam', password='admin2021!@#', db='leefamStock', charset='utf8')

        # 테이블이 존재하지 않는다면 신규 생성
        with self.conn.cursor() as curs:
            sql = """
            CREATE TABLE IF NOT EXISTS crawl_log(
                idx INT(10) NOT NULL AUTO_INCREMENT PRIMARY KEY,
                start_date DATETIME,
                end_date DATETIME,
                flag CHAR(1),
                message NVARCHAR(4000)
            )
            """
            curs.execute(sql)

            sql = """
            CREATE TABLE IF NOT EXISTS company_info(
                code VARCHAR(20),
                company VARCHAR(40),
                last_update DATE,
                PRIMARY KEY (CODE))
            """

            curs.execute(sql)
            sql = """
            CREATE TABLE IF not EXISTS daily_price(
                code VARCHAR(20),
                date DATE,
                open BIGINT(20),
                high BIGINT(20),
                low BIGINT(20),
                close BIGINT(20),
                diff BIGINT(20),
                volume BIGINT(20),
                PRIMARY KEY (CODE, DATE)
            )"""

            curs.execute(sql)
        self.conn.commit()

        self.codes = dict()        
        self.update_comp_info()

    def __del__(self):
        """소멸자: MariaDB 연결 해제"""
        self.conn.close()

    def read_krx_code(self):
        """KRX로부터 상장법인목록 파일을 읽어와서 데이터프레임으로 반환"""
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method='\
              'download&searchType=13'
        krx = pd.read_html(url, header=0)[0]
        krx = krx[['종목코드', '회사명']]
        krx = krx.rename(columns={'종목코드':'code', '회사명':'company'})
        krx.code = krx.code.map('{:06d}'.format)
        return krx

    def update_comp_info(self):
        """종목코드를 company_info 테이블에 업데이트한 후 딕셔너리에 저장"""
        sql = "SELECT * FROM company_info"
        df = pd.read_sql(sql, self.conn)
        for idx in range(len(df)):
            self.codes[df['code'].values[idx]]=df['company'].values[idx]
            
        with self.conn.cursor() as curs:
            sql = "SELECT max(last_update) FROM company_info"
            curs.execute(sql)
            rs = curs.fetchone()
            today = datetime.today().strftime('%Y-%m-%d')

            if rs[0] == None or rs[0].strftime('%Y-%m-%d') < today:
                krx = self.read_krx_code()
                for idx in range(len(krx)):
                    code = krx.code.values[idx]
                    company = krx.company.values[idx]
                    sql = f"REPLACE INTO company_info (code, company, last"\
                          f"_update) VALUES('{code}', '{company}', '{today}')"

                    curs.execute(sql)
                    self.codes[code] = company
                    tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                    print(f"[{tmnow}] #{idx+1:04d} REPLACE INTO company_info "\
                          f"VALUES ({code}, {company}, {today})")
                self.conn.commit()
            print('')

    def read_naver(self, code, company, pages_to_fetch):
        """네이버 금융에서 주식 시세를 읽어서 데이터프레임으로 반환"""
        try:
            url = f"https://finance.naver.com/item/sise.nhn?code={code}"

            # chorme 드라이버를 통한 금융 페이지 호출
            chrome_options = webdriver.ChromeOptions()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')

            driver = webdriver.Chrome(executable_path="/home/lee/leefamStock/Batch/drivers/chromedriver",chrome_options=chrome_options)

            driver.implicitly_wait(10)
            
            driver.get(url)

            # 크롬 새탭 오픈
            driver.find_element_by_tag_name('body').send_keys(Keys.COMMAND + 't')

            # 마지막 페이지를 조회하기 위해 우선 일별 시세 페이지에 연결한다.
            url = f"https://finance.naver.com/item/sise_day.nhn?code={code}"
            driver.get(url)
            doc = driver.page_source
            
            # 일별시세 페이지를 불러오지 못했다면 실행하지 않는다.
            if doc is None:
                return None
                
            html = BeautifulSoup(doc, 'lxml')
            pgrr = html.find('td', class_='pgRR')

            # pgRR태그가 존재하지 않으면 실행하지 않는다.
            if pgrr is None:
                return None

            # 마지막 페이지가 어떻게 되는지 확인
            s = str(pgrr.a['href']).split('=')
            last_page = s[-1]

            #데이터프레임을 구성한다.
            df = pd.DataFrame()

            pages = min(int(last_page), pages_to_fetch)
            for page in range(1, int(pages) + 1):
                page_url = '{}&page={}'.format(url, page)
                driver.get(page_url)
                df = df.append(pd.read_html(driver.page_source, header=0)[0])
                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                print('[{}] {} ({}) : {:04d}/{:04d} pages are downloading...'.format(tmnow, company, code, page, pages), end="\r")
                    
            df = df.rename(columns={'날짜':'date', '종가':'close', '전일비':'diff', '시가':'open', '고가':'high', '저가':'low', '거래량':'volume'})
            df['date'] = df['date'].replace('.','-')

            #차트 출력을 위해 데이터 프레임 가공하기
            df = df.dropna()
            df[['close','diff','open','high','low','volume']] = df[['close','diff','open','high','low','volume']] = df[['close','diff','open','high','low','volume']].astype(int)
            df = df[['date','open','high','low','close','diff','volume']]

            driver.find_element_by_tag_name('body').send_keys(Keys.COMMAND + 'w')
            driver.close()
                
        except Exception as e:
            print('Exception occured : ', str(e))
            return None
        return df

    def replace_into_db(self, df, num, code, company):
        """네이버 금융에서 읽어온 주식 시세를 DB에 REPLACE"""
        with self.conn.cursor() as curs:
            for r in df.itertuples():
                sql = "REPLACE INTO daily_price VALUES ('{}', '{}', {}, {}"\
                      ", {}, {}, {}, {})".format(code, r.date, r.open, r.high, r.low, r.close, r.diff, r.volume)
                      
                curs.execute(sql)
            self.conn.commit()
            print('[{}] #{:04d} {} ({}) : {} rows > REPLACE INTO daily_'\
                  'price [OK]'.format(datetime.now().strftime('%Y-%m-%d %H:%M'), num+1, company, code, len(df)))

    def update_daily_price(self, pages_to_fetch):
        """KRX 상장법인의 주식 시세를 네이버로부터 읽어서 DB에 업데이트"""
        for idx, code in enumerate(self.codes):
            df = self.read_naver(code, self.codes[code], pages_to_fetch)
            if df is None:
                continue
            self.replace_into_db(df, idx, code, self.codes[code])

    def execute_daily(self):
        """실행 즉시 및 매일 오후 다섯시에 daily_price 테이블 업데이트"""
        self.crwal_log_start()
        self.call_slack_message("크롤링 시작합니다!")

        try:
            self.update_comp_info()
            try:
                with open('config.json', 'r') as in_file:
                    config = json.load(in_file)
                    pages_to_fetch = config['pages_to_fetch']
            except FileNotFoundError:
                with open('config.json', 'w') as out_file:
                    pages_to_fetch = 100
                    config = {'pages_to_fetch': 1}
                    json.dump(config, out_file)
            self.update_daily_price(pages_to_fetch)

            self.crwal_log_end('S', 'Success')
            self.call_slack_message("크롤링 Success!!\n크롤링 종료합니다!")
        except Exception as e:
            self.crwal_log_end('E', str(e))
            self.call_slack_message(f"크롤링 Error!!\n {str(e)} 크롤링 종료합니다.")

        tmnow = datetime.now()
        lastday = calendar.monthrange(tmnow.year, tmnow.month)[1]
        
        if tmnow.month == 12 and tmnow.day == lastday:
            tmnext = tmnow.replace(year=tmnow.year+1, month = 1, day=1, hour=17, minute=0, second=0)
        elif tmnow.day == lastday:
            tmnext = tmnow.replace(month=tmnow.month+1, day=1, hour=17, minute=0, second=0)
        else:
            tmnext = tmnow.replace(day=tmnow.day+1, hour=17, minute=0, second=0)
            
        tmdiff = tmnext - tmnow
        secs = tmdiff.seconds

        t = Timer(secs, self.execute_daily)
        print("Waiting for next update ({}) ...".format(tmnext.strftime('%Y-%m-%d %H:%M')))
        t.start()

    def crwal_log_start(self):
        """ 크롤링 시작 로그 기록 """
        global idx

        today = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        with self.conn.cursor() as curs:
            sql = f"INSERT INTO crawl_log(start_date, flag)"\
                F"VALUES ('{ today }', 'P')"
            curs.execute(sql)
            self.conn.commit()

            sql = "SELECT LAST_INSERT_ID()"
            curs.execute(sql)
            rs = curs.fetchone()
            idx = rs[0]

    def crwal_log_end(self, flag, message):
        """ 크롤링 종료 로그 기록 """
        today = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        with self.conn.cursor() as curs:
            sql = f"UPDATE crawl_log SET end_date='{ today }', flag = '{ flag }', message='{ message }'"\
                f"WHERE idx = { idx }"
            curs.execute(sql)
            self.conn.commit()

    def call_slack_message(self, message):
        """ slack 메시지 설정 """
        webhook_url = "https://hooks.slack.com/services/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"

        payload = {"text": f"{message}"}

        requests.post(webhook_url, json=payload)

if __name__ == '__main__':
    dbu = DBUpdater()
    dbu.execute_daily()
