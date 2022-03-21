# How to migrate ETL script from QLikView / Qlik Sense to PySpark Cheatsheet

## QlikView / Qlik Sense to PySpark commands mapping

Before starting the migration code Guidelinies needed to be included to keep the code simple and readable.

### SELECT
First in the ETL should be get only the column you need. It prevent errors in the development and later it is stable in the operating.

_Qlik code snippet_
``` sql 
table_name:
LOAD id,
     name, 
     value 
RESIDENT master_data;
```
---
_Python code snippet_
``` python
table_name = master_data.select(F.col("id"), F.col('name'), F.col('value'))
```

### RENAME COLUMN
_Qlik code snippet_
``` sql 
table_name:
LOAD 
     id as id_core,
     name as name_core,
     value as value_core
RESIDENT master_data;
```
---
_Python code snippet_
``` python
table_name = table_name.withColumnRenamed("ID", "ID_CORE")

table_name = table_name.withColumnRenamed("NAME", "NAME_CORE")

table_name = table_name.withColumnRenamed("VALUE", "VALUE_CORE")
```
``` python
table_name = table_name

.withColumnRenamed("ID", "ID_CORE").withColumnRenamed("NAME", "NAME_CORE").withColumnRenamed("VALUE", "VALUE_CORE")
```

### JOIN
It is important to set what kind of join it is. In the example it is left join 'how="left"'

_Qlik code snippet_
``` sql 
left join(table_name)
LOAD id,
     name, 
     value 
RESIDENT master_data;
```
---
_Python code snippet_
``` python
table_name = table_name.join(master_data, "ID", how="left")
```

### WHERE
_Qlik code snippet_
``` sql 
table_name:
LOAD
  ID,
  NAME,
  VALUE
RESIDENT master_data
WHERE VALUE = '1';
```
---
_Python code snippet_
``` python
table_name = table_name.filter(table_name.VALUE == '1')
```
_Other filter operator_
``` python
# filter <, >, >=, <=
table_name = table_name.filter(table_name.VALUE >= '1')
```
``` python
# combined
table_name = table_name.filter((table_name.VALUE > '1') & (table_name.VALUE < '10'))
```
``` python
# is in, list filter
table_name = table_name.filter(col('VALUE').isin([1, ,2, 3]))
```

### In field changes
_Qlik code snippet_
``` sql 
table_name:
LOAD
  ID,
  NAME,
  if(VALUE = TRUE(), 1, 0) as VALUE
RESIDENT master_data
```
---
_Python code snippet_
``` python
from pyspark.sql import functions as F

true_false_numeric = F.udf(lambda x: 1 if x else 0)

table_name = master_data.withColumn('VALUE', true_false_numeric(master_data.VALUE))
```
For changes in fields it is mandatory to write user defined function. In the example the function maps one or zero to values true or false. To use functions in PySpark the package __"pyspark.sql"__ is required: __"from pyspark.sql import functions as F"__

pyspark.sql functions includes some basic sql function like max and min in field.
_Qlik code snippet_
``` sql 
table_name:
LOAD
  ID,
  NAME,
  max(VALUE) as max_value
RESIDENT master_data
GROUP BY ID, NAME
```
---
_Python code snippet_
``` python
from pyspark.sql import functions as F

table_name = table_name.groupBy(
    'ID', 'NAME'
).agg(F.max('VALUE').alias('max_value'))
```

### Create new columns
_Qlik code snippet_
``` sql 
table_name:
LOAD
  id,
  name,
  'something text' as value
RESIDENT master_data;
```
---
_Python code snippet_
``` python
from pyspark.sql import functions as F


table_name = table_name.withColumn('value', F.lit('something text'))
```

### Combine columns
_Qlik code snippet_
``` sql 
table_name:
LOAD
  name & '-' & value as id,
  name,
  value
RESIDENT master_data;
```
---
_Python code snippet_
``` python
from pyspark.sql import functions as F

table_name = table_name.withColumn('id', F.concat_ws('-', 'name', 'value'))
```

### Order by and Group By combined
To order and aggregate at the same time it is better to use __Window__ function in PySpark

_Qlik code snippet_
```sql
table_name:
LOAD
  NAME,
  FirstValue(VALUE) as max_value
RESIDENT master_data
GROUP BY NAME
ORDER BY DATE desc;
```
---
_Python code snippet_
```python
from pyspark.sql.window import Window

window_get_last_event = Window.partitionBy('NAME').orderBy(F.col('DATE').desc())
table_name = table_name.withColumn('ROW_NUMBER', F.row_number().over(window_get_last_event))
table_name = table_name.filter(F.col('ROW_NUMBER') == 1)
```

### Save / Store as csv
_Qlik code snippet_
```sql
STORE table_name into 'path/to/folder/name.csv'(txt);
```
---
_Python code snippet_
```python
table_name.write.mode('overwrite').option('header', True).csv('s3://bucket_name/folder/name')
```
