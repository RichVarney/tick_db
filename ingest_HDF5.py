"""Extract data from HDF5 file, connect to a new database and insert the data
Method goes through the hierarchical file one member at a time, stores the data into memory (list comprehension) and
inserts directly into a PostgreSQL table

Open the file
  > for each 'group' (match, open, change, done)
      > go to each table (BTC_USD, ETH_EUR etc.)
          > add the symbol, currency and exchange to the product database table, if not already added
          > if there is data in the table, extract it all into one 2d numpy array
          > convert the encoded raw byte data into float values and strings
          > add the product.unique_id to the array by looking it up from the product table
          > insert all the data into the tick_data table

If all data does not fit into memory, look into generator expressions as this will process one-by-one i.e.:
    list_compr = [x**2 for x in range(10)]
    gen_expr = (x**2 for x in range(10))

During testing a 559MB file (26,150,000 rows collected over ~18 days) took 20 minutes to convert and ingest.
The resulting PostgreSQL database was 1,513MB
"""


import os
import h5py
import psycopg2.extras
import config
import timeit
import numpy as np
from datetime import datetime
from io import StringIO


# TODO separate out the code that DROPs and CREATEs the tables so can either replace with new data, or keep appending

db_table_columns = {'match':    ('unique_id', 'product_id', 'side', 'time', 'price', 'size', 'maker_order_id',
                                 'taker_order_id'),
                    'open':     ('unique_id', 'product_id', 'side', 'time', 'price', 'remaining_size', 'order_id'),
                    'change':   ('unique_id', 'product_id', 'side', 'time', 'price', 'old_size', 'new_size',
                                 'order_id'),
                    'done':     ('unique_id', 'product_id', 'side', 'time', 'price', 'remaining_size', 'reason',
                                 'order_id'),
                    'received': ('unique_id', 'order_id', 'order_type', 'side')}


def divide(a, b):
    """custom function to divide two numbers, used by numpy.vectorize"""
    if b == 0:
        return 0.0
    return float(a)/b


def assemble_data_as_string(h5, group, table, db_table):
    """Given the inputs of a HDF5 file, the group and table of the dataset within that file and the database table this
    data is to be inserted into, extract each column from the HDF5 dataset as arrays.
    Combine the arrays into a string of newline separated tuples (CSV format)
    Write the string to a file-like object (StringIO)
    Use Psycopg2 copy_expert to combine a PostgreSQL COPY statement with the file-like object to do a fast COPY
    directly into the database"""
    chunk = 1000000
    number_of_rows = len(h5[group + '/' + table])
    size_array, maker_order_id_array, taker_order_id_array, remaining_size_array, order_id_array, old_size_array, \
    new_size_array, reason_array = [0, 0, 0, 0, 0, 0, 0, 0]
    for count in range(int(round((number_of_rows / chunk) + 0.5))):
        start = count * chunk
        if number_of_rows > start:  # If there are still rows left in the HDF5 file
            end = (count + 1) * chunk  # i.e. list[start:end]
            if number_of_rows < chunk:
                end = number_of_rows  # If reaching the end of the dataset, reassign 'end' variable to the last row
            # Gather data into separate arrays
            db_table_name = db_table[10:]
            if db_table_name in ['match', 'open', 'done', 'change']:
                product_id_array = np.full(end, product_id)
                side_array = np.char.decode(h5[group + '/' + table]['side'][start:end].astype(np.bytes_), 'UTF-8')
                timestamp_array = np.vectorize(datetime.fromtimestamp)(h5[group + '/' + table]['time'][start:end] / 1e6).astype('str')
                price_array = h5[group + '/' + table]['price'][start:end]
            if db_table_name in ['open', 'done', 'change', 'received']:
                try:
                    order_id_array = np.char.decode(h5[group + '/' + table]['order_id'][start:end].astype(np.bytes_), 'UTF-8')
                except ValueError:
                    order_id_array = np.full(end, '00000000-0000-0000-0000-000000000000')
            if db_table_name in ['match', 'open', 'done', 'change', 'received']:
                side_array = np.char.decode(h5[group + '/' + table]['side'][start:end].astype(np.bytes_), 'UTF-8')
            if db_table_name in ['open', 'done']:
                remaining_size_array = np.vectorize(divide)(h5[group + '/' + table]['remaining_size'][start:end], 100000000)
            if db_table_name == 'match':
                size_array = np.vectorize(divide)(h5[group + '/' + table]['size'][start:end], 100000000)
                try:
                    maker_order_id_array = np.char.decode(h5[group + '/' + table]['maker_order_id'][start:end].astype(np.bytes_), 'UTF-8')
                except ValueError:
                    maker_order_id_array = np.full(end, '00000000-0000-0000-0000-000000000000')
                try:
                    taker_order_id_array = np.char.decode(h5[group + '/' + table]['taker_order_id'][start:end].astype(np.bytes_), 'UTF-8')
                except ValueError:
                    taker_order_id_array = np.full(end, '00000000-0000-0000-0000-000000000000')
            if db_table_name == 'change':
                old_size_array = np.vectorize(divide)(h5[group + '/' + table]['old_size'][start:end], 100000000)
                new_size_array = np.vectorize(divide)(h5[group + '/' + table]['new_size'][start:end], 100000000)
            if db_table_name == 'done':
                reason_array = np.char.decode(h5[group + '/' + table]['reason'][start:end].astype(np.bytes_), 'UTF-8')
            if db_table_name == 'received':
                order_type_array = np.char.decode(h5[group + '/' + table]['order_type'][start:end].astype(np.bytes_), 'UTF-8')

            # Write all relevant arrays to a StringIO file-like object
            sio = StringIO()
            if 'open' in db_table:
                sio.writelines(arrays_to_CSV_string(
                        arrays=[product_id_array, side_array, timestamp_array, price_array, remaining_size_array,
                                order_id_array]))
            if 'done' in db_table:
                sio.writelines(arrays_to_CSV_string(
                        arrays=[product_id_array, side_array, timestamp_array, price_array, remaining_size_array,
                                reason_array, order_id_array]))
            if 'match' in db_table:
                sio.writelines(arrays_to_CSV_string(
                        arrays=[product_id_array, side_array, timestamp_array, price_array, size_array,
                                maker_order_id_array, taker_order_id_array]))
            if 'change' in db_table:
                sio.writelines(arrays_to_CSV_string(
                        arrays=[product_id_array, side_array, timestamp_array, price_array, old_size_array,
                                new_size_array, order_id_array]))
            if 'received' in db_table:
                sio.writelines(arrays_to_CSV_string(
                        arrays=[order_id_array, order_type_array, side_array]))

            sio.seek(0)
            # Populate SQL statement with database table column names
            # e.g. "COPY table (column 1, column2, column3 etc) FROM STDIN WITH CSV HEADER"
            sql = "COPY {} {} FROM STDIN WITH CSV HEADER"\
                .format(db_table, str(db_table_columns[db_table[10:]][1:]).replace("'", ""))
            # Copy long SQL statement to table as above
            cur.copy_expert(sql, sio, size=8192)


def arrays_to_CSV_string(**args):
    """Arrange arrays into one string of 'newline' separated tuples (CSV format)"""
    string = '\n'.join([','.join([str(x) for x in variables]) for variables in
                        zip(*[args['arrays'][i] for i in range(len(args['arrays']))])])
    return string


def insert_product_id(symbol, currency, symbol_name, exchange):
    """Using 'symbol', 'currency' and 'exchange' as identifiers, generate a new product ID and insert it into the DB"""
    cur = conn.cursor()
    cur.execute('INSERT INTO product (unique_id, symbol, currency, name, exchange) '
                'VALUES (DEFAULT, \'{}\', \'{}\', \'{}\', \'{}\') '
                'ON CONFLICT DO NOTHING; '
                .format(symbol, currency, symbol_name, exchange))


def populate_product_table(symbol, currency, exchange):
    """Using 'symbol', 'currency' and 'exchange' as identifiers, get the relevant product ID from the 'product' table"""
    product_id = 0
    if h5[group + '/' + table].shape[0] > 0:
        cur = conn.cursor()
        cur.execute('SELECT unique_id, symbol, currency, name, exchange '
                    'FROM product '
                    'WHERE symbol = \'{}\' AND currency = \'{}\' AND exchange = \'{}\';'.format(symbol, currency, exchange))
        try:
            product_id = cur.fetchone()[0]
        except:
            product_id = 0
    return product_id


# Make connection to database
conn = psycopg2.connect(database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS, host=config.DB_HOST)
cur = conn.cursor()
cur.itersize = 10000

# Load SQL schema to create tables.
# Only use this while testing, as in operation, will not want to DROP tables when running this script
# SQL_schema = open('/srv/dev-disk-by-uuid-72faeb4b-1843-416e-b797-438a1f605024/crypto_db/create_tables.sql', 'r').read()
# SQL_schema = open('/Volumes/SSD/projects/crypto_db/create_tables.sql', 'r').read()
# cur.execute(SQL_schema)

# Lookup values to relate a product's symbol with it's full name
symbol_name_lookup = {'BTC': 'Bitcoin', 'ETH': 'Ethereum', 'LTC': 'Litecoin', 'XLM': 'Stellar', 'OMG': 'OMG Network',
                      'DASH': 'Dash', 'EOS': 'EOS'}

# Folder with HDF5 files
# hdf_folder = '/Volumes/SSD/data/webscraper/bulk_ingest'
hdf_folder = '/srv/dev-disk-by-uuid-72faeb4b-1843-416e-b797-438a1f605024/websocket-scraper'

for file_in_folder in os.listdir(hdf_folder):
    if not file_in_folder.startswith('.') and file_in_folder.endswith('.h5'):
        file_path = hdf_folder+'/'+file_in_folder
        filename = h5py.File(file_path, 'r')
        # Given the currently selected file in the folder, see if it has already been uploaded into the database
        cur.execute('SELECT filename, time '
                    'FROM file_log '
                    'WHERE filename LIKE \'%{}\';'.format(file_in_folder))
        filename_check = cur.fetchone()  # look up file_log.filename
        if filename_check is None:  # If there is no record of this filename in the database, continue
            print('Processing {}'.format(file_in_folder))
            # Record name of file, date of upload and the time span of when the data was collected over
            # time_span = os.stat(file_path).st_ctime - os.stat(file_path).st_birthtime
            cur = conn.cursor()
            cur.execute('INSERT INTO file_log (unique_id, filename, time) '
                        'VALUES (DEFAULT, \'{}\', \'{}\')'.format(filename.filename, datetime.now(tz=None)))
            start_time = timeit.default_timer()
            with filename as h5:
                for index, group in enumerate(h5.keys()):
                    for table in h5[group]:
                        print('Loading {} tick data from {} group...'.format(table, group), end='')
                        symbol, currency = table.split('_')
                        insert_product_id(symbol, currency, symbol_name_lookup[symbol], 'Coinbase Pro')
                        product_id = populate_product_table(symbol, currency, 'Coinbase Pro')
                        assemble_data_as_string(h5, group, table, 'tick_data_'+group)
                        conn.commit()
                        print(' time taken: {:0.1f}s'.format(timeit.default_timer() - start_time))

        else:
            print('There was already a record of {} as it was ingested on {}. Didn\'t ingest.'.format(
                    filename_check[0], filename_check[1]))
