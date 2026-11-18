import os
DATABASE_URL=os.getenv('DATABASE_URL','postgresql+psycopg://migrate:migratepass@db:5432/migrationcheck')
REDIS_URL=os.getenv('REDIS_URL','redis://redis:6379/0')
JWT_SECRET=os.environ['JWT_SECRET']
ARCHIVE_DIR=os.getenv('ARCHIVE_DIR','/artifacts/archive')
ARCHIVE_CAPACITY_BYTES=int(os.getenv('ARCHIVE_CAPACITY_BYTES',str(8*1024*1024)))
ARCHIVE_RETENTION_DAYS=int(os.getenv('ARCHIVE_RETENTION_DAYS','30'))
ARCHIVE_PIN_TTL_SECONDS=int(os.getenv('ARCHIVE_PIN_TTL_SECONDS','300'))
