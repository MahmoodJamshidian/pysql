import pysql

DB_HOST="HOST"
DB_USER="USERNAME"
DB_PASSWORD="PASSWORD"
DB_DATABASE="DATABASE"

db = pysql.Database(pysql.mysql_handler(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_DATABASE
))

if db.is_table_exists("persons"):
    t_persons = db.get_table("persons")
else:
    t_persons = db.create_table("persons",
        id=pysql.INTEGER(True, True, True),
        first_name=pysql.VARCHAR(20, required=True),
        last_name=pysql.VARCHAR(20, required=True),
        father_name=pysql.VARCHAR(20),
        phone_number=pysql.VARCHAR(10),
        age=pysql.INTEGER()
    )

t_persons.insert(first_name='Mahmood', last_name='Jamshidian', age=18)
t_persons.insert(first_name='Hossein', last_name='Jalili', father_name='Ahmad')
t_persons.insert(first_name='Ali', last_name='Safara', phone_number="913*******")
t_persons.insert(first_name='Masoud', last_name='Mohammadi')
t_persons.insert(first_name='Alireza', last_name='Jamshidian')
t_persons.insert(first_name='Tarane', last_name='Golshadi')

for name, family in t_persons.select(t_persons.first_name, t_persons.last_name, WHERE=t_persons.id>3):
    print(f"{name} {family}")

t_persons.drop()
