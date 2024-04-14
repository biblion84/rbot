CREATE TABLE IF NOT EXISTS subreddit (
                                         name TEXT PRIMARY KEY,
                                         id TEXT,
                                         subscribers INTEGER,
                                         type TEXT
);

CREATE TABLE if not exists submission (
                                          id TEXT PRIMARY KEY,
                                          author TEXT NOT NULL,
                                          author_created_utc INTEGER NOT NULL,
                                          created_utc INTEGER NOT NULL,
                                          domain TEXT NOT NULL,
                                          is_original_content INTEGER NOT NULL CHECK (is_original_content IN (0, 1)),
                                          is_self INTEGER NOT NULL CHECK (is_self IN (0, 1)),
                                          name TEXT NOT NULL,
                                          num_comments INTEGER NOT NULL,
                                          num_crossposts INTEGER NOT NULL,
                                          over18 INTEGER NOT NULL CHECK (over18 IN (0, 1)),
                                          pinned INTEGER NOT NULL CHECK (pinned IN (0, 1)),
                                          score INTEGER NOT NULL,
                                          subreddit TEXT NOT NULL,
                                          thumbnail TEXT,
                                          title TEXT NOT NULL,
                                          total_awards_received INTEGER NOT NULL,
                                          upvote_ratio REAL NOT NULL,
                                          url TEXT,
                                          url_overridden_by_dest TEXT,
                                          view_count INTEGER NOT NULL,
                                          FOREIGN KEY (subreddit) REFERENCES subreddit(name)
);


DROP TABLE submission;
DROP TABLE subreddit;


select count(*) from submission;


select * from submission limit 100 ;


select * from subreddit limit 100;

select count(*) from subreddit;
select count(*) from submission;


CREATE INDEX IF NOT EXISTS subreddit_idx ON submission (subreddit);

select count(*), subreddit from submission group by subreddit order by count(*) desc limit 100;

SHOW shared_buffers;

ALTER SYSTEM SET shared_buffers = '512MB';

SELECT pg_size_pretty(pg_database_size('postgres')) as db_size;


ANALYZE verbose comment2;

ANALYZE verbose comment;

set enable_seqscan = off;

show random_page_cost;

set random_page_cost = 1.2;

explain
select count(*), author from comment2 group by author order by count(*) desc limit 100;

explain
select  author from comment group by author limit 100;


SELECT
    table_schema,
    table_name,
    pg_size_pretty(pg_table_size(full_table_name)) AS table_size,
    pg_size_pretty(pg_indexes_size(full_table_name)) AS indexes_size,
    pg_size_pretty(pg_total_relation_size(full_table_name)) AS total_size
FROM (
         SELECT table_schema, table_name, table_schema || '.' || table_name AS full_table_name
         FROM information_schema.tables
         WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
           AND table_type = 'BASE TABLE'
     ) AS all_tables
ORDER BY pg_total_relation_size(full_table_name) DESC;


select count(*) from comment;

select * from comment order by id desc limit 1;


select * from comment limit 100;


CREATE INDEX IF NOT EXISTS comment_author_idx on comment(author);

explain
select count(1) from comment where subreddit = 'nba';

explain
select count(*) from comment group by author;


explain
select count(*), subreddit from comment where author = 'I_might_be_weasel' group by subreddit order by count(*) desc;

ANALYZE comment;


ALTER TABLE comment2 ADD COLUMN tsv TSVECTOR;

UPDATE comment2 SET tsv = to_tsvector('english', text);
CREATE INDEX idx_fts_on_comment2 ON comment2 USING GIN(tsv);


select count(*) from comment2;

explain
select * from comment2 where tsv @@ to_tsquery('english', 'motherfucker & you');


CREATE INDEX IF NOT EXISTS comment_subreddit_author_idx on comment(subreddit, author);


show effective_cache_size;


explain
SELECT COUNT(DISTINCT author) FROM comment;


explain
SELECT COUNT(DISTINCT author) FROM comment;


SELECT COUNT(DISTINCT author) FROM comment;

SHOW random_page_cost;

ALTER SYSTEM SET
    max_connections = '20';
ALTER SYSTEM SET
    shared_buffers = '8GB';
ALTER SYSTEM SET
    effective_cache_size = '24GB';
ALTER SYSTEM SET
    maintenance_work_mem = '2047MB';
ALTER SYSTEM SET
    checkpoint_completion_target = '0.9';
ALTER SYSTEM SET
    wal_buffers = '16MB';
ALTER SYSTEM SET
    default_statistics_target = '500';
ALTER SYSTEM SET
    random_page_cost = 1.05;
ALTER SYSTEM SET
    work_mem = '26214kB';
ALTER SYSTEM SET
    huge_pages = 'try';
ALTER SYSTEM SET
    min_wal_size = '4GB';
ALTER SYSTEM SET
    max_wal_size = '16GB';
ALTER SYSTEM SET
    max_worker_processes = '16';
ALTER SYSTEM SET
    max_parallel_workers_per_gather = '8';
ALTER SYSTEM SET
    max_parallel_workers = '16';
ALTER SYSTEM SET
    max_parallel_maintenance_workers = '4';

explain
select * from comment order by score desc limit 100;



explain
select * from comment order by score desc;