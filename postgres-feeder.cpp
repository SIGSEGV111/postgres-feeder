#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <malloc.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>

#define SYSERR(expr) (([&](){ const auto r = ((expr)); if( (long)r == -1L ) { throw #expr; } else return r; })())

// suse
#include <pgsql/libpq-fe.h>

// // debian
// #include <postgresql/libpq-fe.h>

/*
	CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

	create table sensor_data
	(
		time TIMESTAMPTZ NOT NULL,
		location text not null,
		sensor   text not null,
		measurand text not null,
		value double precision not null
	);

	SELECT create_hypertable('sensor_data', 'time');
*/

static char* mkstrcpy(const char* const str)
{
	const size_t sz = strlen(str) + 1;
	char* n = (char*)malloc(sz);
	if(n == nullptr) throw "unable to allocate memory for a string-copy operation";
	memcpy(n, str, sz);
	return n;
}

class TPostgreSQL
{
	protected:
		PGconn* conn;

	public:
		bool verbose;

		void Execute(const char* const sql, ...)
		{
			va_list list;
			char buffer[256];

			va_start(list, sql);
			vsnprintf(buffer, sizeof(buffer), sql, list);
			va_end(list);

			PGresult* res = PQexec(conn, buffer);
			if(verbose)
				fprintf(stderr, "[PQ] %s\n", PQcmdStatus(res));

			if(PQresultStatus(res) != PGRES_COMMAND_OK && PQresultStatus(res) != PGRES_TUPLES_OK)
			{
				PQclear(res);
				throw mkstrcpy(PQerrorMessage(conn));
			}

			PQclear(res);
		}

		void Copy(const void* const data, const int sz_data, const char* const sql, ...)
		{
			va_list list;
			char buffer[256];

			va_start(list, sql);
			vsnprintf(buffer, sizeof(buffer), sql, list);
			va_end(list);

			PGresult* res = PQexec(conn, buffer);

			if(PQresultStatus(res) != PGRES_COPY_IN)
			{
				char* err = mkstrcpy(PQerrorMessage(conn));
				PQclear(res);
				throw err;
			}

			PQclear(res);

			if(PQputCopyData(conn, (const char*)data, sz_data) != 1)
				throw "PQputCopyData() failed";

			if(PQputCopyEnd(conn, nullptr) != 1)
				throw "PQputCopyEnd() failed";

			res = PQgetResult(conn);

			if(PQresultStatus(res) != PGRES_COMMAND_OK)
			{
				char* err = mkstrcpy(PQerrorMessage(conn));
				PQclear(res);
				throw err;
			}

			PQclear(res);
		}

		TPostgreSQL(const char* const connstr = "")
		{
			this->conn = PQconnectdb(connstr);
			if(PQstatus(conn) != CONNECTION_OK)
				throw mkstrcpy(PQerrorMessage(conn));
			this->verbose = false;
		}

		~TPostgreSQL()
		{
			PQfinish(this->conn);
		}
};

static volatile bool do_run = true;

static void OnSignal(int)
{
	do_run = false;
}

int main(int argc, char* argv[])
{
	signal(SIGTERM, &OnSignal);
	signal(SIGINT,  &OnSignal);
	signal(SIGQUIT, &OnSignal);
	signal(SIGHUP,  &OnSignal);
	signal(SIGPIPE, &OnSignal);

	close(STDOUT_FILENO);

	try
	{
		if(argc < 2)
			throw "need at least one argument: <table name>";

		SYSERR(lseek(STDIN_FILENO, SEEK_SET, 0));

		TPostgreSQL pq(argc > 2 ? argv[2] : "");
		pq.verbose = true;

		fprintf(stderr, "[INFO] postgres-feeder ready!\n");

		while(do_run)
		{
			SYSERR(flock(STDIN_FILENO, LOCK_EX));

			struct stat st;
			SYSERR(fstat(STDIN_FILENO, &st));

			if(st.st_size > 0)
			{
				const void* const buffer = SYSERR(mmap(nullptr, st.st_size, PROT_READ, MAP_SHARED|MAP_POPULATE, STDIN_FILENO, 0));
				pq.Copy(buffer, st.st_size, "COPY \"%s\" FROM STDIN WITH (FORMAT CSV, DELIMITER ';', QUOTE '\"', ESCAPE '\\')", argv[1]);
				SYSERR(munmap((void*)buffer, st.st_size));

				SYSERR(lseek(STDIN_FILENO, SEEK_SET, 0));
				SYSERR(ftruncate(STDIN_FILENO, 0));
			}

			SYSERR(flock(STDIN_FILENO, LOCK_UN));

			// sleep 10s
			usleep(10 * 1000 * 1000);
		}

		fprintf(stderr, "\n[INFO] bye!\n");
		return 0;
	}
	catch(char* const err)
	{
		fprintf(stderr, "[ERROR] %s\n", err);
		free(err);
		return 2;
	}
	catch(const char* const err)
	{
		fprintf(stderr, "[ERROR] %s\n", err);
		return 1;
	}
	return 100;
}
