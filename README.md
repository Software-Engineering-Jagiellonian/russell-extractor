# frege-extractor
Frege module that assigns repository's sources to programming languages and passes them to analyzers.

The application listens for RabbitMQ messages from queue `extract` in the following format:

```
{
    "repo_id": "<repo_id>"
}
```
After receiving a message, it scans the directory named `<repo_id>` 
for any source files by their extensions. Then, it inserts in the database proper entries in tables
`repository_language`, and `repository_language_file`. Finally, for each language found in the repository,
 the same RabbitMQ message is passed to its analyzer queue.

## Running

Run this application with the following command:

`docker run -it jagiellonian/frege-extractor <environmental variables> -v 
<path to repositories directory>:\repo_downloads`

Use `-v` option to mount volume that contains repositories' directory.
Target volume inside the container is called 'repo_downloads'.

## Environmental variables

Run this application with following environmental variables:

- `RMQ_HOST` - RabbitMQ host
- `RMQ_PORT` - RabbitMQ port
- `RMQ_REJECTED_PUBLISH_DELAY` - number of seconds to wait between retrying sending output message
- `DB_HOST` - Postgres server host
- `DB_PORT` - Postgres server port
- `DB_DATABASE` - database name
- `DB_USERNAME` - database user name
- `DB_PASSWORD` - database user password