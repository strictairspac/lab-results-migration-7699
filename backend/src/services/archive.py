import hashlib,json,logging,os,threading,uuid
from contextlib import contextmanager
from datetime import datetime,timezone,timedelta
from sqlalchemy import text,func
from src.config.database import Session,engine
from src.config.settings import ARCHIVE_DIR,ARCHIVE_CAPACITY_BYTES,ARCHIVE_RETENTION_DAYS,ARCHIVE_PIN_TTL_SECONDS
from src.models.archive import ArchiveObject,ArchiveRef,ArchivePin
from src.models.run import Run
LOCK_KEY=41094109
_LOCAL=threading.RLock()
class CapacityRejected(Exception):pass
def now():return datetime.now(timezone.utc).replace(tzinfo=None)
def canonical_bytes(backup):return json.dumps(backup,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode('utf-8')
def _objects_dir():
 p=os.path.join(ARCHIVE_DIR,'objects');os.makedirs(p,exist_ok=True);return p
def _path(digest):return os.path.join(_objects_dir(),digest)
@contextmanager
def archive_lock():
 # 归档登记/回收/固定全部经过同一把锁：文件与行的增删不会交叉，已登记正文不会被并发回收误删
 if engine.dialect.name=='postgresql':
  conn=engine.connect()
  try:
   conn.execute(text('SELECT pg_advisory_lock(:k)'),{'k':LOCK_KEY});conn.commit();yield
  finally:
   try:conn.execute(text('SELECT pg_advisory_unlock(:k)'),{'k':LOCK_KEY});conn.commit()
   except Exception:logging.exception('archive unlock')
   conn.close()
 else:
  with _LOCAL:yield
def _store_file(digest,body):
 # 先写临时文件再原子改名；改名后、登记前停机只留孤儿文件，不占额度，重试覆盖同内容
 _objects_dir()
 tmp=os.path.join(ARCHIVE_DIR,'tmp-'+uuid.uuid4().hex)
 with open(tmp,'wb') as f:f.write(body);f.flush();os.fsync(f.fileno())
 os.replace(tmp,_path(digest))
def _used(s):return s.query(func.coalesce(func.sum(ArchiveObject.size_bytes),0)).scalar()
def _expired(s,at):
 live_pins={p.digest for p in s.query(ArchivePin).filter(ArchivePin.expires_at>at).all()}
 refs={}
 for r in s.query(ArchiveRef).all():refs.setdefault(r.digest,[]).append(r)
 return [d for d,rs in refs.items() if d not in live_pins and all(r.retain_until<=at for r in rs)]
def _reclaim_locked(s,at=None):
 # 只回收全部引用都已到期且无恢复固定的正文；先删行再扫文件，停机重试不会重复释放额度
 at=at or now();reclaimed=[];freed=0
 for d in _expired(s,at):
  obj=s.get(ArchiveObject,d)
  if not obj:continue
  size=obj.size_bytes
  run_ids=[r.run_id for r in s.query(ArchiveRef).filter_by(digest=d).all()]
  for run in s.query(Run).filter(Run.id.in_(run_ids)).all():
   res=dict(run.result or {});ba=dict(res.get('backup_archive') or {})
   if ba.get('digest')==d:ba.update(status='reclaimed',reclaimed_at=at.isoformat());res['backup_archive']=ba;run.result=res
  s.query(ArchiveRef).filter_by(digest=d).delete();s.delete(obj)
  reclaimed.append({'digest':d,'size_bytes':size,'runs':run_ids});freed+=size
 s.query(ArchivePin).filter(ArchivePin.expires_at<=at).delete()
 return reclaimed,freed
def _sweep(s):
 keep={o.digest for o in s.query(ArchiveObject).all()}
 for name in os.listdir(_objects_dir()):
  if name not in keep:
   try:os.remove(os.path.join(_objects_dir(),name))
   except OSError:logging.exception('sweep %s',name)
 if os.path.isdir(ARCHIVE_DIR):
  for name in os.listdir(ARCHIVE_DIR):
   if name.startswith('tmp-'):
    try:os.remove(os.path.join(ARCHIVE_DIR,name))
    except OSError:logging.exception('sweep tmp %s',name)
def archive_backup(run_id,backup,retention_days=None):
 # 相同正文只登记一份；容量不足先回收过期内容，仍不足则拒绝且不留部分状态
 body=canonical_bytes(backup);digest=hashlib.sha256(body).hexdigest();size=len(body)
 retain=now()+timedelta(days=retention_days or ARCHIVE_RETENTION_DAYS)
 reclaimed=[]
 with archive_lock():
  with Session() as s:
   if not s.get(ArchiveObject,digest):
    if size>ARCHIVE_CAPACITY_BYTES-_used(s):
     reclaimed,_=_reclaim_locked(s)
     if size>ARCHIVE_CAPACITY_BYTES-_used(s):
      s.rollback()
      raise CapacityRejected(f'归档容量不足：正文{size}字节，回收过期内容后剩余{max(ARCHIVE_CAPACITY_BYTES-_used(s),0)}字节，总容量{ARCHIVE_CAPACITY_BYTES}字节')
    _store_file(digest,body)
    s.add(ArchiveObject(digest=digest,size_bytes=size))
   ref=s.query(ArchiveRef).filter_by(run_id=run_id).first()
   if ref:ref.digest=digest;ref.retain_until=retain
   else:s.add(ArchiveRef(run_id=run_id,digest=digest,retain_until=retain))
   s.commit()
 info={'status':'archived','digest':digest,'size_bytes':size,'retain_until':retain.isoformat()}
 if reclaimed:info['reclaimed']=[x['digest'] for x in reclaimed]
 return info
def read_body(digest):
 try:
  with open(_path(digest),'rb') as f:body=f.read()
 except FileNotFoundError:raise KeyError(digest)
 if hashlib.sha256(body).hexdigest()!=digest:raise ValueError('归档正文摘要不一致，拒绝读取')
 return body
def pin(digest,ttl_seconds=None):
 # 恢复/下载期间固定正文；锁内确认对象存在，之后的回收必然看到此固定
 token=uuid.uuid4().hex
 with archive_lock():
  with Session() as s:
   if not s.get(ArchiveObject,digest):raise KeyError(digest)
   s.add(ArchivePin(token=token,digest=digest,expires_at=now()+timedelta(seconds=ttl_seconds or ARCHIVE_PIN_TTL_SECONDS)))
   s.commit()
 return token
def unpin(token):
 with Session() as s:s.query(ArchivePin).filter_by(token=token).delete();s.commit()
def collect():
 with archive_lock():
  with Session() as s:
   reclaimed,freed=_reclaim_locked(s);s.commit()
   _sweep(s)
   used=_used(s)
 return {'reclaimed':reclaimed,'freed_bytes':freed,'used_bytes':used,'capacity_bytes':ARCHIVE_CAPACITY_BYTES}
def status():
 at=now()
 with Session() as s:
  objs=s.query(ArchiveObject).order_by(ArchiveObject.created_at.desc()).all()
  refs={}
  for r in s.query(ArchiveRef).all():refs.setdefault(r.digest,[]).append(r)
  pins={p.digest for p in s.query(ArchivePin).filter(ArchivePin.expires_at>at).all()}
  names={r.id:r.name for r in s.query(Run).all()}
  objects=[{'digest':o.digest,'size_bytes':o.size_bytes,'created_at':o.created_at.isoformat() if o.created_at else None,'file_present':os.path.exists(_path(o.digest)),'pinned':o.digest in pins,'refs':[{'run_id':x.run_id,'run_name':names.get(x.run_id),'retain_until':x.retain_until.isoformat(),'expired':x.retain_until<=at} for x in refs.get(o.digest,[])]} for o in objs]
  used=sum(o.size_bytes for o in objs)
  runs=[{'run_id':r.id,'run_name':r.name,'archive':(r.result or {}).get('backup_archive')} for r in s.query(Run).filter(Run.status=='completed').order_by(Run.id.desc()).limit(100).all() if (r.result or {}).get('backup_archive')]
 return {'capacity_bytes':ARCHIVE_CAPACITY_BYTES,'used_bytes':used,'free_bytes':ARCHIVE_CAPACITY_BYTES-used,'object_count':len(objects),'objects':objects,'runs':runs}
