import psycopg2
from pprint import pprint


# Default database connection parameters
HOST = 'localhost'
DATABASE = 'database'
USER = 'user'
PASSWORD = 'password'

# Check for local settings
try:
    from local_settings import *
except ImportError:
    pass


def add_map_labels(table, labelfield):
    """
    Adds column 'map_label_txt' with title case text of given labelfield to given table.
    In case of roads table, USPS standardized abbreviations are used.
    """

    # Standardized USPS abbrevations dictionary
    # https://www.usps.com/send/official-abbreviations.htm
    USPS_abbr = {
        'Road': 'Rd',
        'Street': 'St',
        'Avenue': 'Ave',
        'Parkway': 'Pkwy',
        'Drive': 'Dr',
        'Lane': 'Ln',
        'Terrace': 'Ter',
        'Turnpike': 'Tpke',
        'Circle': 'Cir',
        'Way': 'Way',
        'Hill': 'Hl',
        'Court': 'Ct',
        'Highway': 'Hwy',
        'Alley': 'Aly',
        'Place': 'Pl',
    }

    conn = None

    try:
        conn = psycopg2.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD)
        cur = conn.cursor()

        # create column
        cur.execute("ALTER TABLE %s DROP COLUMN IF EXISTS map_label_txt" % (table))
        cur.execute("ALTER TABLE %s ADD COLUMN map_label_txt character varying(100)" % (table))

        # select labelfield
        cur.execute("SELECT gid, %s FROM %s WHERE %s IS NOT NULL" % (labelfield, table, labelfield))

        # build label dictionary
        labels = {}
        while True:
            row = cur.fetchmany(500)
            if row == []:
                break
            for i,orig_txt in row:
                label = orig_txt.title()
                label = label.replace("'S", "'s")
                # use USPS abbrevations for road suffixes
                if table == 'roads':
                    for k, v in USPS_abbr.items():
                        label = label.replace(k, v)
                labels[label] = orig_txt

        pprint(labels)
        
        # update map_label_txt
        print "updating rows..."
        cur.executemany("UPDATE " + table + " SET map_label_txt = %s WHERE " + labelfield + " = %s", labels.items())

        # commit transactions
        conn.commit()
        print "done"

    except psycopg2.DatabaseError, e:
        print 'Error: %s' % e
        return None

    finally:
        if conn:
            conn.close()

