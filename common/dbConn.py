import pymysql, json

class DBConn:
    @classmethod
    def set_db_conn(cls):
        mariadb_con = DBConn.get_conn_info()

        if type(mariadb_con) == 'string' and mariadb_con == 'Error':
            return 'connection Error!!!'

        return pymysql.connect(host=mariadb_con.get('host'), user=mariadb_con.get('user'),
                                    password=mariadb_con.get('password'), db=mariadb_con.get('db'),
                                    charset=mariadb_con.get('charset'))

    @classmethod
    def get_conn_info(cls):
        try:
            with open('config.json', 'r', encoding="utf8") as in_file:
                config = json.load(in_file)
                mariadb_con = config['mariadb_con']
                return mariadb_con
        except FileNotFoundError:
            '''error log '''
            return 'Error'

