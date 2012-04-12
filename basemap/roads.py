import psycopg2

# Default settings

# Database connection parameters
HOST = 'localhost'
DATABASE = 'database'
USER = 'user'
PASSWORD = 'password'


def add_map_labels():
    """
    Adds column 'map_label_txt' with title case and USPS standardized street name abbrevations.
    """

    # Standardized USPS abbrevatiosn dictionary
    # https://www.usps.com/send/official-abbreviations.htm
    USPS_abbr = {
        'ROAD': 'Rd',
        'STREET': 'St',
        'AVENUE': 'Ave',
        'PARKWAY': 'Pkwy',
        'DRIVE': 'Dr',
        'LANE': 'Ln',
        'TERRACE': 'Ter',
        'TURNPIKE': 'Tpke',
        'CIRCLE': 'Cir',
        'WAY': 'Way',
        'HILL': 'Hl',
        'COURT': 'Ct',
        'HIGHWAY': 'Hwy',
        'ALLEY': 'Aly',
    }

    conn = None

    try:
        conn = psycopg2.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD)
        cur = conn.cursor()

        # create column
        cur.execute("ALTER TABLE roads DROP COLUMN IF EXISTS map_label_txt")
        cur.execute("ALTER TABLE roads ADD COLUMN map_label_txt character varying(100)")

        # select street names
        cur.execute("SELECT gid, street_nam FROM roads WHERE street_nam IS NOT NULL")

        # build streetnames label dictionary
        streetnames = {}
        while True:
            row = cur.fetchmany(500)
            if row == []:
                break
            for i,streetname in row:
                label = streetname
                for k, v in USPS_abbr.items():
                    label = label.replace(k, v)
                streetnames[label.title()] = streetname

        cur.executemany("UPDATE roads SET map_label_txt = %s WHERE street_nam = %s", streetnames.items())

        # commit transactions
        conn.commit()

    except psycopg2.DatabaseError, e:
        print 'Error: %s' % e
        return None

    finally:
        if conn:
            conn.close()

# Local settings
try:
    from local_settings import *
except ImportError:
    pass

