from sqlalchemy import Column,Integer,String,DateTime,ForeignKey,func
from src.config.database import Base
class ArchiveObject(Base):
 __tablename__='archive_objects'
 digest=Column(String(64),primary_key=True)
 size_bytes=Column(Integer,nullable=False)
 created_at=Column(DateTime,server_default=func.now())
class ArchiveRef(Base):
 __tablename__='archive_refs'
 id=Column(Integer,primary_key=True)
 run_id=Column(Integer,ForeignKey('runs.id'),unique=True,nullable=False)
 digest=Column(String(64),nullable=False)
 retain_until=Column(DateTime,nullable=False)
 created_at=Column(DateTime,server_default=func.now())
class ArchivePin(Base):
 __tablename__='archive_pins'
 token=Column(String(64),primary_key=True)
 digest=Column(String(64),nullable=False)
 expires_at=Column(DateTime,nullable=False)
 created_at=Column(DateTime,server_default=func.now())
