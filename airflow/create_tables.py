import configparser
import psycopg2

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    
    with conn.cursor() as cur:
        cur.execute(open("create_tables.sql", "r").read())
   
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()