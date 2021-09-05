import pandas as pd
from datetime import datetime, timedelta
from common import dbConn
import Analyzer

class DualMomentum:
    def __init__(self):
        """생성자 : KRX 종목코드(codes)를 구하기 위한 MarketDB 객체 생성"""
        self.mk = Analyzer.MarketDB()

    def get_rltv_momentum(self, start_date, end_date, stock_count):
        """특정 기간 동안 수익률이 제일 높았던 stock_count 개의 종목들(상대 모멘텀)
            - start_date    : 상대 모멘텀을 구할 시작일자
            - end_date      : 상대 모멘텀을 구할 종료일자
            - stock_cunt    : 상대 모멘텀을 구할 종목수
        :return:
        """
        connection = dbConn.DBConn.set_db_conn()
        cursor = connection.cursor()

        start_date = self.get_max_date(cursor, start_date)

        if (start_date == 'NotData'):
            print("start_date returned None")
            return

        end_date = self.get_max_date(cursor, end_date)

        if (end_date == 'NotData'):
            print("end_date returned None")
            return

        # KRX 종목별 수익률을 구해서 2차원 리스트 형태로 추가
        rows = []
        columns = ['code', 'company', 'old_price', 'new_price', 'returns']
        for _, code in enumerate(self.mk.codes):
            old_price = self.get_close_price(cursor, code, start_date)
            if (old_price == 'NotData'):
                continue

            new_price = self.get_close_price(cursor, code, end_date)
            if (new_price == 'NotData'):
                continue

            returns = (new_price/old_price - 1) * 100
            rows.append([code, self.mk.codes[code], old_price, new_price, returns])

        # 상대 모멘텀 프레임을 생성한 후 수익률 순으로 출력
        df = pd.DataFrame(rows, columns=columns)
        df = df[['code', 'company', 'old_price', 'new_price', 'returns']]
        df = df.sort_values(by='returns', ascending=False)
        df = df.head(stock_count)
        df.index = pd.Index(range(stock_count))

        connection.close()
        print(df)
        print(f"\nRelative momentum ({start_date} ~ {end_date}) : "\
              f"{df['returns'].mean():.2f}%\n")

        return df

    def get_abs_momentum(self, rltv_momentum, start_date, end_date):
        """특정 기간 동안 상대 모멘텀에 투자했을 때의 평균 수익률 (절대 모멘텀)
            - rltv_momentum     : get_rlt_momentum() 함수의 리턴값 (상대 모멘텀)
            - start_date        : 절대 모멘텀을 구할 시작일자
            - end_date          : 절대 모멘텀을 구할 종료일자
        """
        stockList = list(rltv_momentum['code'])

        connection = dbConn.DBConn.set_db_conn()
        cursor = connection.cursor()

        start_date = self.get_max_date(cursor, start_date)

        if (start_date == 'NotData'):
            print("start_date returned None")
            return

        end_date = self.get_max_date(cursor, end_date)

        if (end_date == 'NotData'):
            print("end_date returned None")
            return

        # 상대 모멘텀의 종목별 수익률을 구해서 2차원 리스트 형태로 추가
        rows = []
        columns = ['code', 'company', 'old_price', 'new_price', 'returns']
        for _, code in enumerate(stockList):
            old_price = self.get_close_price(cursor, code, start_date)
            if (old_price == 'NotData'):
                continue

            new_price = self.get_close_price(cursor, code, end_date)
            if (new_price == 'NotData'):
                continue

            returns = (new_price / old_price - 1) * 100
            rows.append([code, self.mk.codes[code], old_price, new_price, returns])

        # 상대 모멘텀 프레임을 생성한 후 수익률 순으로 출력
        df = pd.DataFrame(rows, columns=columns)
        df = df[['code', 'company', 'old_price', 'new_price', 'returns']]
        df = df.sort_values(by='returns', ascending=False)

        connection.close()
        print(df)
        print(f"\nAbsolute momentum ({start_date} ~ {end_date}) : " \
            f"{df['returns'].mean():.2f}%\n")

        return df

    def get_max_date(self, cursor, date):
        # 사용자가 입력한 시작일자를 DB에서 조회되는 일자로 보정
        sql = f"select max(date) from daily_price where date <= '{date}'"
        cursor.execute(sql)
        result = cursor.fetchone()

        if (result[0] is None):
            return 'NotData'

        return result[0].strftime('%Y-%m-%d')

    def get_close_price(self, cursor, code, date):
        sql = f"select close from daily_price "\
            f"where code='{code}' and date='{date}'"
        cursor.execute(sql)
        result = cursor.fetchone()

        if (result is None):
            return 'NotData'

        return int(result[0])