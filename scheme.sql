drop table if exists queue;
drop table if exists hashes;
drop table if exists paths;
drop table if exists unsupported;

create table unsupported (full text);

create table queue (
	url text,
	hostname varchar(256),
	starthost varchar(256),
	retry_count int
);

create table paths (
	hostname varchar(256),
	proto varchar(16),
	filepath varchar(512),
	full text,
	blobid varchar(256)
);

create index hostname on queue (hostname);
create index url on queue (url);
