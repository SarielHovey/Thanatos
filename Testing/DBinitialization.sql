# sudo service mysql start
# mysql -u sec_user -p

USE securities_master;
/*
Exchange: Info of where data stems from, like NYSE, NASDAQ
Data Vendor: Information of data vendors, like Alpha Vantage, Tushare
Symbol: List of ticket symbols and company information
Daily Price: Daily pricing information for each security
*/
CREATE TABLE exchange ( id int NOT NULL AUTO_INCREMENT, 
                       abbrev varchar(32) NOT NULL, 
                       name varchar(255) NOT NULL, 
                       city varchar(255) NULL, 
                       country varchar(255) NULL, 
                       currency varchar(64) NULL, 
                       timezone_offset time NULL, 
                       created_date datetime NOT NULL, 
                       last_updated_date datetime NOT NULL, 
                       PRIMARY KEY (id)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
                                    
CREATE TABLE data_vendor ( id int NOT NULL AUTO_INCREMENT, 
                          name varchar(64) NOT NULL, 
                          website_url varchar(255) NULL, 
                          support_email varchar(255) NULL, 
                          created_date datetime NOT NULL, 
                          last_updated_date datetime NOT NULL, 
                          PRIMARY KEY (id)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
                                       
CREATE TABLE symbol ( id int NOT NULL AUTO_INCREMENT, 
                     exchange_id int NULL, 
                     ticker varchar(32) NOT NULL, 
                     instrument varchar(64) NOT NULL, 
                     name varchar(255) NULL, 
                     sector varchar(255) NULL, 
                     currency varchar(32) NULL, 
                     created_date datetime NOT NULL, 
                     last_updated_date datetime NOT NULL, 
                     PRIMARY KEY (id),
                     KEY index_exchange_id (exchange_id)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
                                            
CREATE TABLE daily_price ( id int NOT NULL AUTO_INCREMENT, 
                          data_vendor_id int NOT NULL, 
                          symbol_id int NOT NULL, 
                          price_date datetime NOT NULL, 
                          created_date datetime NOT NULL, 
                          last_updated_date datetime NOT NULL, 
                          open_price decimal(19,4) NULL, 
                          high_price decimal(19,4) NULL, 
                          low_price decimal(19,4) NULL, 
                          close_price decimal(19,4) NULL, 
                          adj_factor decimal(19,10) NULL, 
                          volume bigint NULL, 
                          PRIMARY KEY (id), KEY index_data_vendor_id (data_vendor_id), KEY index_symbol_id (symbol_id)) 
                          ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

#-------------------------------------------------------------------------------------------------------------------------
                                                                                                            
# if use sqlite, then use the below instead
                                                                                                            
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

CREATE TABLE data_vendor ( 
    id INT PRIMARY KEY, 
    name varchar(64) NOT NULL, 
    website_url varchar(255), 
    support_email varchar(255), 
    created_date datetime, 
    last_updated_date datetime
    );
    
CREATE TABLE symbol ( 
    id int PRIMARY KEY, 
    exchange_id int KEY, 
    ticker varchar(32) NOT NULL, 
    instrument varchar(64) NOT NULL, 
    name varchar(255), 
    sector varchar(255), 
    currency varchar(32), 
    created_date datetime NOT NULL, 
    last_updated_date datetime NOT NULL
    );
    
CREATE TABLE daily_price ( 
    id int PRIMARY KEY, 
    data_vendor_id int KEY NOT NULL, 
    symbol_id int KEY NOT NULL, 
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
