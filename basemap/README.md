# MAPC Basemap data manipulations

Routine tasks to be executed before data is used to render MAPC's basemap.

## Usage

Install requirements:

    $ pip install -r requirements.txt

Either directly edit `basemap.py` or add a `local_settings.py` to provide database connection parameters in form of:

    HOST = 'localhost'
    DATABASE = 'database'
    USER = 'user'
    PASSWORD = 'password'

Import the module to a Python shell:

    $ python
    >>> import basemap

### Add pretty map labels

Adds column 'map_label_txt' with title case text of given labelfield to given table. In case of table named roads, USPS standardized abbreviations are used.

Execute the function with a *table* and *labelfield* argument:

    >>> basemap.add_map_labels("mytable","mylabeltxtfield")