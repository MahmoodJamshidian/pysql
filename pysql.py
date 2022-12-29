from mysql import connector as mysql_connector
from typing import Union, List, Dict, Tuple, Callable, Any

class db_handler:
    dbname: str
    def __init__(self): ...
    def execute(self, cmd, args=[]): ...
    def close(self): ...
    def __del__(self): ...

class mysql_handler(db_handler):
    def __init__(self, host="localhost", user="root", password="", database="test"):
        self.db = mysql_connector.connect(host=host, user=user, password=password, database=database)
        self.cursor = self.db.cursor()
        self.dbname = database
    
    def execute(self, cmd, args: list = []) -> dict:
        try:
            self.cursor.execute(cmd, args)
        except Exception as ex:
            return Exception(str(ex))
        try:
            self.db.commit()
        except:
            pass
        res = dict(lastrowid=self.cursor.lastrowid, rowcount=self.cursor.rowcount)
        try:
            res['data'] = self.cursor.fetchall()
        except:
            pass
        return res

    def close(self):
        try:
            self.cursor.close()
        except:
            pass
        try:
            self.db.close()
        except:
            pass

    def __del__(self):
        self.close()

def stringify(msg:str):
    return msg.replace('\n', '\\n').replace('\r', '\\r').replace('"', '\\"').replace("'", "\\'")

class str_query(str):
    def __init__(self, text:str) -> None:
        super().__init__()
        self.text = text

    def __and__(self, other):
        return str_query(self.text + " AND " + str(other))

    def __or__(self, other):
        return str_query(self.text + " OR " + str(other))

class DataType:
    def __lt__(self, other):
        if isinstance(other, Union[int, float]):
            return str_query(f"{self.table}.{self.cname} < {other}")
        elif isinstance(other, str):
            return str_query(f"{self.table}.{self.cname} < '{stringify(other)}'")
        elif isinstance(other, DataType):
            return str_query(f"{self.table}.{self.cname} < {other.table}.{other.cname}")
        else:
            raise TypeError(f"{other} is not a valid type for comparison")

    def __le__(self, other):
        if isinstance(other, Union[int, float]):
            return str_query(f"{self.table}.{self.cname} <= {other}")
        elif isinstance(other, str):
            return str_query(f"{self.table}.{self.cname} <= '{stringify(other)}'")
        elif isinstance(other, DataType):
            return str_query(f"{self.table}.{self.cname} <= {other.table}.{other.cname}")
        else:
            raise TypeError(f"{other} is not a valid type for comparison")

    def __eq__(self, other):
        if isinstance(other, Union[int, float]):
            return str_query(f"{self.table}.{self.cname} = {other}")
        elif isinstance(other, str):
            return str_query(f"{self.table}.{self.cname} = '{stringify(other)}'")
        elif isinstance(other, DataType):
            return str_query(f"{self.table}.{self.cname} = {other.table}.{other.cname}")
        else:
            raise TypeError(f"{other} is not a valid type for comparison")
    
    def __ne__(self, other):
        if isinstance(other, Union[int, float]):
            return str_query(f"{self.table}.{self.cname} != {other}")
        elif isinstance(other, str):
            return str_query(f"{self.table}.{self.cname} != '{stringify(other)}'")
        elif isinstance(other, DataType):
            return str_query(f"{self.table}.{self.cname} != {other.table}.{other.cname}")
        else:
            raise TypeError(f"{other} is not a valid type for comparison")

    def __gt__(self, other):
        if isinstance(other, Union[int, float]):
            return str_query(f"{self.table}.{self.cname} > {other}")
        elif isinstance(other, str):
            return str_query(f"{self.table}.{self.cname} > '{stringify(other)}'")
        elif isinstance(other, DataType):
            return str_query(f"{self.table}.{self.cname} > {other.table}.{other.cname}")
        else:
            raise TypeError(f"{other} is not a valid type for comparison")
    
    def __ge__(self, other):
        if isinstance(other, Union[int, float]):
            return str_query(f"{self.table}.{self.cname} >= {other}")
        elif isinstance(other, str):
            return str_query(f"{self.table}.{self.cname} >= '{stringify(other)}'")
        elif isinstance(other, DataType):
            return str_query(f"{self.table}.{self.cname} >= {other.table}.{other.cname}")
        else:
            raise TypeError(f"{other} is not a valid type for comparison")

    def like(self, other):
        if isinstance(other, str):
            return str_query(f"{self.table}.{self.cname} LIKE '{stringify(other)}'")
        else:
            raise TypeError(f"{other} is not a valid type for comparison")



class INTEGER(DataType):
    def __init__(self, primary_key=False, auto_increment=False, required=False, default=None, auto_number_start=1):
        self.primary_key = primary_key
        self.auto_increment = auto_increment
        self.required = required
        self.default = default
        self.auto_number_start = auto_number_start

    def __str__(self, multiple=False):
        res = "INTEGER" + " PRIMARY KEY" * self.primary_key * (not multiple) + " AUTO_INCREMENT" * self.auto_increment + " NOT NULL" * self.required
        if self.default is not None and self.default != "":
            if type(self.default) in [int, float]:
                res += " DEFAULT " + str(self.default)
            else:
                res += " DEFAULT '" + str(self.default) + "'"
        return res

class TEXT(DataType):
    def __init__(self, primary_key=False, required=False, default=None):
        self.primary_key = primary_key
        self.required = required
        self.default = default

    def __str__(self, multiple=False):
        res = "TEXT" + " PRIMARY KEY" * self.primary_key * (not multiple) + " NOT NULL" * self.required
        if self.default is not None:
            res += " DEFAULT '" + str(self.default).replace("'", "\\\'").replace('"', '\\\"') + "'"
        return res

class VARCHAR(DataType):
    def __init__(self, length, primary_key=False, required=False, default=None):
        self.length = length
        self.primary_key = primary_key
        self.required = required
        self.default = default

    def __str__(self, multiple=False):
        res = f"VARCHAR({self.length})" + " PRIMARY KEY" * self.primary_key * (not multiple) + " NOT NULL" * self.required
        if self.default is not None:
            res += " DEFAULT '" + str(self.default).replace("'", "\\\'").replace('"', '\\\"') + "'"
        return res

class BOOLIAN(DataType):
    def __init__(self, primary_key=False, required=False, default=None):
        self.primary_key = primary_key
        self.required = required
        self.default = default

    def __str__(self, multiple=False):
        res = "BOOL" + " PRIMARY KEY" * self.primary_key * (not multiple) + " NOT NULL" * self.required
        if self.default is not None and self.default in [True, False]:
            if type(self.default) in [int, bool]:
                res += " DEFAULT " + str(bool(self.default)).upper()
            else:
                raise Exception("Default value for BOOL must be True or False")
        return res

class JSON(DataType):
    def __init__(self, primary_key=False, required=False):
        self.primary_key = primary_key
        self.required = required

    def __str__(self, multiple=False):
        res = "JSON" + " PRIMARY KEY" * self.primary_key * (not multiple) + " NOT NULL" * self.required
        return res


class select_row:
    def __init__(self, data:list, columns:list):
        self.data = list(data)
        self.columns = list(columns)

    def __getitem__(self, item):
        if isinstance(item, str):
            index = self.columns.index(item)
            return self.data[index]
        elif isinstance(item, int):
            return self.data[item]
        else:
            raise ValueError(f"{item} is not a valid type for indexing")

    def __getattribute__(self, __name: str) -> Any:
        try:
            return object.__getattribute__(self, __name)
        except:
            if __name in object.__getattribute__(self, "columns"):
                index = self.columns.index(__name)
                return self.data[index]
            else:
                raise AttributeError


class select_table:
    def __init__(self, data:list, columns:list):
        self.data = data
        self.columns = columns

    def __getitem__(self, item):
        if isinstance(item, str):
            index = self.columns.index(item)
            return [d[index] for d in self.data]
        elif isinstance(item, int):
            return select_row(self.data[item], self.columns)
        else:
            raise ValueError(f"{item} is not a valid type for indexing")

    def __getattribute__(self, __name: str):
        try:
            return super().__getattribute__(__name)
        except:
            if __name in object.__getattribute__(self, "columns"):
                return self[__name]
            else:
                raise AttributeError

    def count(self):
        return len(self.data)

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        return iter(self.data)

    def __next__(self):
        return next(self.data)

class Database: ...

class table:
    def __init__(self, tname:str, db: Database, columns:dict):
        self.tname = tname
        self.db = db
        self.columns = columns
        self.require_cols = [n for n, c in self.columns.items() if not (isinstance(c, INTEGER) and c.auto_increment)]
        self._closed = False

    def __getattribute__(self, __name: str):
        if __name in (columns:=object.__getattribute__(self, "columns")):
            columns[__name].table = self.tname
            columns[__name].cname = __name
            return columns[__name]
        else:
            return object.__getattribute__(self, __name)

    def __getitem__(self, item):
        return self.select(item)


    def select(self, *args, WHERE=None):
        if self._closed:
            raise Exception("Table was not found")

        tables = []
        [tables.append(arg.table) for arg in args if arg.table not in tables]
        if len(tables) < 1:
            tables.append(self.tname)
        sql = f"SELECT {', '.join([f'{arg.table}.{arg.cname}' for arg in args]) if len(args) > 0 else '*'} FROM {', '.join(tables)}{f' WHERE {WHERE}' if not WHERE is None else ''}"
        return select_table(self.db.handler.execute(sql)['data'], args if len(args) > 0 else self.columns.keys())
        
        
    def insert(self, *args, **kwargs):
        if self._closed:
            raise Exception("Table was not found")
        if len(args) == 0 and len(kwargs) == 0:
            raise Exception("No data provided")

        column_data = {k: v.default for k, v in self.columns.items() if k in self.require_cols}
        for index in range(len(args)):
            if index >= len(self.require_cols):
                raise Exception("Too many data provided")
            column_data[self.require_cols[index]] = args[index]
        for k, v in kwargs.items():
            if k not in self.columns:
                raise Exception("Unknown column: " + k)
            column_data[k] = v
        for k, v in column_data.items():
            if v is None:
                if self.columns[k].required:
                    raise Exception("Column " + k + " is required")

        sql = "INSERT INTO " + self.tname + " (" + ", ".join(column_data.keys()) + ") VALUES (" + ", ".join(["%s"] * len(column_data)) + ")"
        return self.db.handler.execute(sql, list(column_data.values()))['lastrowid']

    def update(self, WHERE, **kwargs):
        if self._closed:
            raise Exception("Table was not found")
        if WHERE is None:
            raise Exception("No WHERE clause provided")
        if len(kwargs) == 0:
            raise Exception("No data provided")
        for k, v in kwargs.items():
            if k not in self.columns:
                raise Exception("Unknown column: " + k)
            if v is None:
                if self.columns[k].required:
                    raise Exception("Column " + k + " is not equal to None")
        sql = "UPDATE " + self.tname + " SET " + ", ".join([f"{k} = %s" for k in kwargs.keys()]) + " WHERE " + WHERE
        return self.db.handler.execute(sql, list(kwargs.values()))['rowcount']

    def delete(self, WHERE=None):
        if self._closed:
            raise Exception("Table was not found")
        sql = "DELETE FROM " + self.tname
        if not WHERE is None:
            sql += " WHERE " + WHERE
        return self.db.handler.execute(sql)['rowcount']

    def drop(self):
        if self._closed:
            raise Exception("Table was not found")
        sql = "DROP TABLE " + self.tname
        self.db.handler.execute(sql)
        self._closed = True


class Database:
    def __init__(self, handher: db_handler):
        self.handler = handher
        self.dbname = self.handler.dbname

    def create_table(self, tname:str, **tcolumns):
        sql = "CREATE TABLE " + tname + " ("
        columns = []
        primary_keys = []
        auto_number_start = 0
        for key, value in tcolumns.items():
            if type(value) in [INTEGER, VARCHAR, TEXT, BOOLIAN, JSON]:
                columns.append(key + " " + value.__str__(multiple=True))
                if value.primary_key:
                    primary_keys.append(key)
                if type(value) == INTEGER and value.auto_increment:
                    auto_number_start = value.auto_number_start
            else:
                raise Exception("Invalid column type")
        sql += ", ".join(columns)
        if len(primary_keys) > 0:
            sql += ", PRIMARY KEY (" + ", ".join(primary_keys) + ")"
        sql += ")" + (" AUTO_INCREMENT=" + str(auto_number_start) if auto_number_start > 0 else "")
        self.handler.execute(sql)
        return table(tname, self, tcolumns)

    def get_table(self, tname:str):
        if self.is_table_exists(tname):
            return table(tname, self, self.get_table_columns(tname))
        else:
            raise Exception("Table does not exist")

    def load_tables_from_dict(self, tables:dict):
        _tables = []
        for tname, tcolumns in tables.items():
            if not self.is_table_exists(tname):
                cols = {}
                for cname, cdata in tcolumns.items():
                    _type = None
                    match cdata["type"]:
                        case "int":
                            _type = INTEGER
                        case "varchar":
                            _type = VARCHAR
                        case "text":
                            _type = TEXT
                        case "boolian":
                            _type = BOOLIAN
                        case "json":
                            _type = JSON
                        case _:
                            raise Exception("Invalid column type")
                    cdata.pop("type")
                    cols[cname] = _type(**cdata)
                _tables.append(self.create_table(tname, **cols))
            else:
                _tables.append(self.get_table(tname))
        return _tables

    def drop_table(self, tname:str):
        if self.is_table_exists(tname):
            sql = "DROP TABLE IF EXISTS " + tname
            self.handler.execute(sql)
            return True
        else:
            return False

    def query(self, sql:str):
        return self.handler.execute(sql)['data']

    @property
    def tables(self):
        return [row[0] for row in self.handler.execute("SHOW TABLES")['data']]
    
    def is_table_exists(self, tname:str):
        return tname in self.tables

    def get_table_columns(self, tname:str) -> dict:
        cols = {}
        for row in self.handler.execute("SELECT DATA_TYPE, COLUMN_NAME, COLUMN_DEFAULT, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH, COLUMN_KEY, EXTRA from INFORMATION_SCHEMA.COLUMNS where table_schema = %s and table_name = %s", (self.dbname, tname,))['data']:
            match row:
                case ('int', cname, default, is_nullable, None, key, extra):
                    cols[cname] = INTEGER(primary_key=key == "PRI", required=is_nullable == "NO", default=default, auto_increment="auto_increment" in extra)
                case ('varchar', cname, default, is_nullable, length, key, extra):
                    cols[cname] = VARCHAR(length, primary_key=key == "PRI", required=is_nullable == "NO", default=default)
                case ('text', cname, default, is_nullable, clenght, key, extra):
                    cols[cname] = TEXT(primary_key=key == "PRI", required=is_nullable == "NO")
                case ('bool', cname, default, is_nullable, None, key, extra):
                    cols[cname] = BOOLIAN(primary_key=key == "PRI", required=is_nullable == "NO", default=default)
                case ('json', cname, default, is_nullable, None, key, extra):
                    cols[cname] = JSON(primary_key=key == "PRI", required=is_nullable == "NO")

        return cols

    def __del__(self):
        self.close()

    def close(self):
        self.handler.close()
