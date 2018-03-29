# nasdaq-symbols
These Racket programs will download the NASDAQ symbol file from the NASDAQ FTP and insert the symbols into a PostgreSQL database. The intended usage is:

```bash
$ racket extract.rkt
$ racket transform-load.rkt
```

The provided schema.sql file shows the expected schema within the target PostgreSQL instance. This process assumes you can write to a /var/tmp/nasdaq folder.
