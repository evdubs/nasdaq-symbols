# nasdaq-symbols
These Racket programs will download the NASDAQ symbol file from the NASDAQ FTP and insert the symbols into a PostgreSQL database. The intended usage is:

```bash
$ racket extract.rkt
$ racket transform-load.rkt
```

The provided schema.sql file shows the expected schema within the target PostgreSQL instance. This process assumes you can write to a /var/tmp/nasdaq folder. This process also assumes that you are running transform-load sequentially starting from the beginning of your data. This should be done as the `last_seen` column just be overwritten with the date of the file and not try to figure out if the current value is greater than the file date value. This can probably be easily changed, but I am lazy. Pull requests are welcome.

### Dependencies

It is recommended that you start with the standard Racket distribution. With that, you will need to install the following packages:

```bash
$ raco pkg install --skip-installed gregor threading
```
