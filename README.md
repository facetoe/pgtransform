# PG Transform

PG Transform is a set of tools I created for solving a particular problem: discovering and fixing schema differences between many hundreds of Postgresql databases.

The tools work by taking a database with a "golden schema" and comparing it against the other databases. The output of the comparison step is a tree of DiffNodes with each individual node representing a single difference. This tree is then passed as input to the transformation program which processes each node, applying any 'strategies' that match. Once this is complete, and provided there is a strategy for each and every difference, the schemas will again be identical. 

All this is achieved with the following three tools.

### pg-pickle

The pg-pickle program processes the database with the "golden schema" and outputs a Python pickle file. This is the template that will be compared to the other databases.

```
⇒ ./pg-pickle -h
Usage: pg-pickle [options]

Options:
  -h, --help            show this help message and exit
  --ignore-columns=IGNORE_COLUMNS
                        Columns to be ignored, specified as a comma seperated
                        list. Wildcards can be used, eg, *ignore*
  --ignore-tables=IGNORE_TABLES
                        Tables to be ignored, specified as a comma seperated
                        list. Wildcards can be used, eg, *ignore*
  -o OUT_PATH, --out=OUT_PATH
                        Path to output file
  --dbname=DBNAME       Database name
  --dbuser=DBUSER       Database user
  --dbpass=DBPASS       Database password
  --dbhost=DBHOST       Database host
  --dbport=DBPORT       Database port

```

### pg-compare

The pg-compare program handles comparing databases against the template. This can be done in parallel, and the output can be printed or exported to a sqlite database. 


```
Usage: pg-compare [options]

Options:
  -h, --help            show this help message and exit
  -c CONFIG, --config=CONFIG
                        Config file
  --ignore-columns=IGNORE_COLUMNS
                        Columns to be ignored, specified as a comma seperated
                        list. Wildcards can be used, eg, *ignore*
  --ignore-tables=IGNORE_TABLES
                        Tables to be ignored, specified as a comma seperated
                        list. Wildcards can be used, eg, *ignore*
  --pickle-path=PICKLE_PATH
                        Path to pickled database
  --max-threads=MAX_THREADS
                        Maximum number of databases to process in parallel
  -o OUT_PATH, --out=OUT_PATH
                        Path to output file
  --output-type=OUTPUT_TYPE
                        Allowed values: ('stdout', 'sqlite')

```

### pg-transform

The pg-transform program does the work of modifying the target schemas to bring them back in line with the template. Like pg-compare, it can be run in parallel against many databases.

```
Usage: pg-transform [options]

Options:
  -h, --help            show this help message and exit
  -c CONFIG, --config=CONFIG
                        Config file
  --max-threads=MAX_THREADS
                        Maximum number of databases to process in parallel
  --commit              Whether or not to commit changes
  --pickle-path=PICKLE_PATH
                        Path to pickled database

```

## Strategies

The project contains several example strategies, however they are intended as examples and are not intended for production use. It is recommended that you write your own strategies to be confident the changes you are applying are correct for your specific situation.

## Tutorial

In this tutorial I will demonstrate how we can use these tools to bring a group of database schemas back in line. We will use the dvdrental example database from http://www.postgresqltutorial.com/load-postgresql-sample-database/.

First things first, let's build the test databases:

```bash
example/build-tutorial-env.sh
```

After executing the script you should have the following databases:

```
vagrant@pgtest:~/tutorial$ psql -l | grep dvdrental
 dvdrental          | postgres | UTF8     | en_US.UTF-8 | en_US.UTF-8 | 
 dvdrental_modified | postgres | UTF8     | en_US.UTF-8 | en_US.UTF-8 | 
```

The dvdrental tutorial database is our known good 'golden schema'. Let's build a template pickle of this database with pg-pickle:

```bash
⇒ ./pg-pickle --dbname dvdrental --dbuser postgres --dbpass postgres --dbhost tutorial --out /tmp/
Pickling: dvdrental to /tmp/dvdrental.pickle
```

As a sanity check, let's confirm that pg-compare returns no differences when executed against the newly created "dvdrental_modified" database:

```bash
⇒ ./pg-compare --config example/example_config.ini --pickle-path /tmp/dvdrental.pickle
Comparing: dvdrental -> dvdrental_modified
Writing results for: dvdrental_modified
```

As expected, no differences are returned. Now, let's modify the schema:

```bash
vagrant@pgtest:~/tutorial$ psql dvdrental_modified -c 'ALTER TABLE actor ALTER COLUMN first_name DROP NOT NULL'
```

And run pg-compare again:

```bash
⇒ ./pg-compare --config example/example_config.ini --pickle-path /tmp/dvdrental.pickle
Comparing: dvdrental -> dvdrental_modified
Writing results for: dvdrental_modified
dvdrental:
    actor:
        first_name:
             -> {is_nullable}: expected: False, found: True
```

Here we can see the `is_nullable` attribute of the `actor.first_name` field is different to the template schema, as expected. Let's apply another schema change:

```bash
psql dvdrental_modified -c 'ALTER TABLE actor ALTER COLUMN last_update DROP DEFAULT'
```

And run pg-compare again:

```bash
⇒ ./pg-compare --config example/example_config.ini --pickle-path /tmp/dvdrental.pickle
Comparing: dvdrental -> dvdrental_modified
Writing results for: dvdrental_modified
dvdrental:
    actor:
        first_name:
             -> {is_nullable}: expected: False, found: True
        last_update:
             -> {column_default}: expected: 'now()', found: None
```

Again, the difference is picked up. Let's execute pg-transform and bring the schema back into line:

```bash
⇒ ./pg-transform --config example/example_config.ini --pickle-path /tmp/dvdrental.pickle --commit
Processing: dvdrental_modified
Comparing: dvdrental_modified
Transforming: dvdrental_modified
Applying strategy: ColumnDefaultStrategy
	--> SETTING actor.last_update DEFAULT to now()

Applying strategy: DatatypeStrategy

Applying strategy: NotNullableStrategy
	--> Setting actor.first_name to NOT NULL

Changes committed!
```

And run pg-compare one last time:

```bash
./pg-compare --config example/example_config.ini --pickle-path /tmp/dvdrental.pickle
Comparing: dvdrental -> dvdrental_modified
Writing results for: dvdrental_modified
```

No differences are registered. 