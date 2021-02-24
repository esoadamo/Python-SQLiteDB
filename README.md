# SQLiteDB

A wrapper for Python's inbuilt SQLite with multithreading support.

## Database usage

```python
from sqlitedb import SQLiteDB
db = SQLiteDB('my.sqlitedb')
for record in db.execute("SELECT * FROM table"):
    print(record)
```

## IndexedDB usage
This package also includes a class that allows you to use your SQLite db as a Python dictionary:

```python
from sqlitedb import IndexedDBManager

# will use/create the db file
dictionary = IndexedDBManager('my.sqlitedb')

# will create a table db_test and there a row called foo with value bar
dictionary['test']['foo'] = 'bar'

# also support anything that can be jsonized
dictionary['test']['list'] = ['1', 2, True]
print(dictionary['test']['list'])  # prints ['1', 2, True]

# and deleting
del dictionary['test']['list']  # deletes the record
del dictionary['test']  # drops whole table
```
