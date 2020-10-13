import sqlite3


def main(path:str ="~/Thanatos/Data/securities_master.db"):
    """
    Used to initial SQLite db for Thanatos under python.
    """
    con = sqlite3.connect(path)
    cur = con.cursor()
    
    cur.execute( """
        CREATE TABLE exchange ( 
        id INT PRIMARY KEY, 
        abbrev VARCHAR(32) NOT NULL, 
        name VARCHAR(255) NOT NULL, 
        city VARCHAR(255), 
        country VARCHAR(255), 
        currency VARCHAR(64), 
        timezone_offset DATETIME, 
        created_date DATETIME NOT NULL, 
        last_updated_date DATETIME NOT NULL
        );
        """ )        
    cur.execute( """
        CREATE TABLE data_vendor ( 
        id INT PRIMARY KEY, 
        name varchar(64) NOT NULL, 
        website_url varchar(255), 
        support_email varchar(255), 
        created_date datetime, 
        last_updated_date datetime
        );
        """ )
    cur.execute( """
        CREATE TABLE symbol ( 
        id int PRIMARY KEY, 
        exchange_id int, 
        ticker varchar(32) NOT NULL, 
        instrument varchar(64) NOT NULL, 
        name varchar(255), 
        sector varchar(255), 
        currency varchar(32), 
        created_date datetime NOT NULL, 
        last_updated_date datetime NOT NULL
        );
        """ )
    cur.execute( """
        CREATE TABLE daily_price ( 
        id int PRIMARY KEY, 
        data_vendor_id int NOT NULL, 
        symbol_id int NOT NULL, 
        price_date datetime NOT NULL, 
        created_date datetime NOT NULL, 
        last_updated_date datetime NOT NULL, 
        open_price decimal(19,4), 
        high_price decimal(19,4), 
        low_price decimal(19,4), 
        close_price decimal(19,4), 
        adj_factor decimal(19,10), 
        volume bigint NULL 
        );
        """ )
        
    con.commit()
    
    
if __name__ == "__main__":
    main()
    print("SQLite db initialized successuflly in ./Data/ !")

    
