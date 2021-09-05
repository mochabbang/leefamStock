import pandas as pd
from datetime import datetime, timedelta
from common import commonFunc, dbConn

class MarketDB:
    def __init__(self):
        """생성자 : MariaDB 연결 및 종목코드 딕셔너리 생성"""
        self.conn = dbConn.DBConn.set_db_conn()
        self.codes = {}
        self.get_comp_into()

    def __del__(self):
        """소멸자 : MariaDB 연결해제"""
        self.conn.close()

    def get_comp_into(self):
        """company_info 테이블에서 읽어와서 codes에 저장"""
        sql = "SELECT * FROM company_info"
        krx = pd.read_sql(sql, self.conn)
        for idx in range(len(krx)):
            self.codes[krx['code'].values[idx]] = krx['company'].values[idx]

    def get_daily_price(self, code, start_date=None, end_date=None):
        """KRX 종목별 시세를 데이터프레임 형태로 반환
            - code:     KRX 종목코드('005930') 또는 상장기업명('삼성전자')
            - start_date : 조회 시작일, 미입력 시 1년전 오늘
            - end_date : 조회 종료일, 미입력 시 오늘날짜
        """
        comFun = commonFunc.commonFunc()

        # start_date 미입력 로직
        if start_date is None:
            one_year_ago = datetime.today() - timedelta(days=365)
            start_date = one_year_ago.strftime('%Y-%m-%d')
            print("start_date is initialized to '{}'".format(start_date))
        else:
            start_date = comFun.convert_date_format(start_date, "start_date")

        if (end_date is None):
            end_date = datetime.today().strftime('%Y-%m-%d')
            print("end_date is initialized to '{}'".format(end_date))
        else:
            end_date = comFun.convert_date_format(end_date, "end_date")

        codes_keys = list(self.codes.keys())
        codes_values = list(self.codes.values())
        if code in codes_keys:
            pass
        elif code in codes_values:
            idx = codes_values.index(code)
            code = codes_keys[idx]
        else:
            print("ValueError: Code({}) doesn't exists.".format(code))

        sql = f"SELECT * FROM daily_price WHERE code = '{code}'"\
            f" and date >= '{start_date}' and date <= '{end_date}'"
        df = pd.read_sql(sql, self.conn)
        df.index = df['date']
        return df