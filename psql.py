#akses data ke database
#cara pakai library psycopg2, kalau mau akses data/ lakukan query ke postgresql pake library ini

import psycopg2 as pg # library untuk akses database, ini library postgree

#credential
host = "35.xxx.xx.xxx"
port = "5xxx"
database = "postgres"
user = "xxxxxres"
password = "xxxxxxxx"
dsn = "dbname=" + database + " user=" + user + " host=" + host + " port=" + port + " password=" + password

#dsn = data source name

#conn = pg.connect(dsn) ------------ ini untuk connect db, syntaxnya juga bisa pakai seperti dibawah ini
conn = pg.connect(dbname=database,host=host, port=port, user=user,password=password)

 # fungsi untuk baca dari database
def read_from_db(conn, query): #ngambil 2 parameter, conect dan query
    cur = conn.cursor() #cur untuk mengarahkan ke database supaya bisa jalanin query, conn.cursor adalah library psycopg
    cur.execute(query)
    data = cur.fetchall()
    cur.close() # setiap bikin cursor langsung di close
    return data

#fungsi untuk eksekusi query
def exec_db(conn, query, data=None):
    cur = conn.cursor()
    cur.execute(query, data)
    conn.commit() #harus di commit untuk menyimpan perubahan atau supaya mendokumentasikan di DB
    cur.close()

# fungsi untuk baca data customer
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

#untuk memanggil atau menampilkan hasil query customer, coba perintah dibawah ini (data....., print.....)
#data = read_customer()
#print(data)

write_customer()
#update_customer()