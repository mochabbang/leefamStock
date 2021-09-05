import re

class commonFunc:
    def convert_date_format(self, date, type):
        date_lst = re.split('\D+', date)
        if date_lst[0] == '':
            date_lst = date_lst[1:]
        year = int(date_lst[0])
        month = int(date_lst[1])
        day = int(date_lst[2])

        if year < 1900 or year > 2200:
            return f"ValueError: {type}({year:d}) is wrong."
        if month < 1 or month > 12:
            return f"ValueError: {type}({month:d}) is wrong."
        if day < 1 or day > 31:
            return f"ValueError: {type}({day:d}) is wrong."

        return f"{year:04d}-{month:02d}-{day:02d}"