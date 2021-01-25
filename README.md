# Dal DbInspector

Helper to create [PyDAL](https://github.com/web2py/pydal) table definitions from legacy databases.

Created mainly for Postresql, it should work also with Sqlite, easy to customize.

## Usage

### Definition

```
from pydal import DAL
from daldbi import DbInspector

uri = 'pgsql://postgres@127.0.0.1/mydb'
db = DAL(uri)

dbi = DbInspector(db)
```

### Get table list

```
dbi.tables

['table1',
'table2'
...]
```

### Print DAL table definition
    
```
print(dbi.table_def('table1', with_id=True))
```

Output:
```
db.define_table('table1',
		Field('id', type='id'),
		Field('user', type='string'),
		Field('ip', type='string'),
		Field('access_date', type='datetime'),
		Field('memo', type='text'),		
		rname='public.table1',
        migrate=migrate)
```

### Define the table directly

```
dbi._define_table('table1')
db.table1
```

Output:
```
<Table table1 (id, user, ip, access_date, memo)>
```