import psycopg2 as pg 

#credential
host = "35.xxx.xx.xxx"
port = "5xxx"
database = "postgres"
user = "xxxxxres"
password = "xxxxxxxx"
dsn = "dbname=" + database + " user=" + user + " host=" + host + " port=" + port + " password=" + password

conn = pg.connect(dbname=database,host=host, port=port, user=user,password=password)

def read_from_db(conn, query): 
    cur = conn.cursor() 
    cur.execute(query)
    data = cur.fetchall()
    cur.close() 
    return data

def exec_db(conn, query, data=None):
    cur = conn.cursor()
    cur.execute(query, data)
    conn.commit() 
    cur.close()

def read_customer():
    query = """
            select customer_unique_id,customer_id, customer_zip_code_prefix, customer_city, customer_state 
            from olist_customers_dataset_csv
            limit 100
    """
    data=read_from_db(conn, query)    
    allrows = []
    for row in data:
        elmt={} 
        elmt["customer_id"] = row[1]
        elmt["customer_unique_id"] = row[0]
        elmt["customer_zip_code_prefix"] = row[2]
        elmt["customer_city"] = row[3]
        elmt["customer_state"] = row[4]
        allrows.append(elmt)
    return allrows

def write_customer():
    query = """
        insert into olist_customers_dataset_csv(customer_unique_id,customer_id, customer_zip_code_prefix, customer_city, customer_state)
        values(%s,%s,%s,%s,%s)
    """
    exec_db(conn, query, ('1234567890', '888889', '15345', 'Surabaya', 'SU') )
    print("berhasil insert data")


def update_customer():
    query = """
        update olist_customers_dataset_csv
        set customer_city='Medan'
        where customer_id='888889'
    """
    exec_db(conn, query)
    print("berhasil update data")

write_customer()
#update_customer()
